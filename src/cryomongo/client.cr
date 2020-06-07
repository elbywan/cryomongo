require "socket"
require "db"
require "./database"
require "./messages/**"
require "./commands/**"
require "./error"
require "./concerns"
require "./read_preference"
require "./sdam/**"
require "./uri"

class Mongo::Client
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  MIN_WIRE_VERSION = 6
  MAX_WIRE_VERSION = 8

  UNACKNOWLEDGED_WRITE_PROHIBITED_OPTIONS = {
    "hint",
    "collation",
    "bypass_document_validation",
    "array_filters",
  }

  getter! topology : SDAM::TopologyDescription
  getter options : Options

  @@lock = Mutex.new(:reentrant)
  @pools : Hash(SDAM::ServerDescription, DB::Pool(Mongo::Connection)) = Hash(SDAM::ServerDescription, DB::Pool(Mongo::Connection)).new
  @monitors : Array(SDAM::Monitor) = Array(SDAM::Monitor).new
  @socket_check_interval : Time::Span = 5.seconds
  @last_scan : Time = Time::UNIX_EPOCH
  @min_heartbeat_frequency : Time::Span = 500.milliseconds
  @topology_update = Channel(Nil).new

  def initialize(connection_string : String = "mongodb://localhost:27017", *, start_monitoring = true)
    seeds, @options, @credentials = Mongo::URI.parse(connection_string)

    if read_pref = @options.read_preference
      @read_preference = ReadPreference.new(
        mode: read_pref,
        max_staleness_seconds: @options.max_staleness_seconds,
        tags: @options.read_preference_tags.map { |tags|
          bson = BSON.new
          tags.split(',').each { |tag|
            key, value = tag.split(':')
            bson[key] = value
          }
          bson
        }
      )
    end

    @topology = SDAM::TopologyDescription.new(self, seeds.map(&.address), @options)
    topology.servers.each { |server|
      add_monitor(server, start_monitoring: start_monitoring)
    }
  end

  def add_monitor(server_description : SDAM::ServerDescription, *, start_monitoring = true)
    monitor = SDAM::Monitor.new(self, server_description, @credentials, @options.heartbeat_frequency || 10.seconds)
    @monitors << monitor
    if start_monitoring
      spawn monitor.scan
    end
  end

  def command(
    command cmd,
    ignore_errors = false,
    write_concern : WriteConcern? = @write_concern,
    read_concern : ReadConcern? = @read_concern,
    read_preference : ReadPreference? = @read_preference,
    server_description : SDAM::ServerDescription? = nil,
    **args
  )
    args = WithWriteConcern.mix_write_concern(cmd, args, write_concern)
    args = WithReadConcern.mix_read_concern(cmd, args, read_concern)

    if WithReadPreference.must_use_primary_command?(cmd, args)
      read_preference = ReadPreference.new(mode: "primary")
    else
      read_preference = @read_preference || read_preference
      read_preference = read_preference || ReadPreference.new(mode: "primary")
    end

    server_description ||= server_selection(cmd, args, read_preference)
    connection = get_connection(server_description)

    args = WithReadPreference.mix_read_preference(cmd, args, read_preference, topology, server_description)

    unacknowledged = false
    if concern = args["options"]?.try(&.["write_concern"]?)
      unacknowledged = concern.w == 0 && !concern.j
    end

    if unacknowledged
      prohibited_option = nil
      UNACKNOWLEDGED_WRITE_PROHIBITED_OPTIONS.each { |option|
        if opt = args["options"]?.try(&.has_key? option)
          prohibited_option = opt
          break
        elsif opt = args["updates"]?.try(&.any? &.has_key? option)
          prohibited_option = opt
          break
        elsif opt = args["deletes"]?.try(&.any? &.has_key? option)
          prohibited_option = opt
          break
        end
      }
      raise Mongo::Error.new("Option #{prohibited_option} is prohibited when performing an unacknowledged write.") if prohibited_option
    end

    body, sequences = cmd.command(**args)
    flag_bits = unacknowledged ? Messages::OpMsg::Flags::MoreToCome : Messages::OpMsg::Flags::None
    op_msg = Messages::OpMsg.new(body, flag_bits: flag_bits)
    sequences.try &.each { |key, documents|
      op_msg.sequence(key.to_s, contents: documents)
    }

    connection.send(op_msg)

    return nil if unacknowledged

    op_msg = connection.receive(ignore_errors: ignore_errors)

    result = cmd.result(op_msg.body)

    if result.is_a? Cursor
      result.server_description = server_description
    end

    result
  ensure
    release_connection(connection) if connection
  end

  def database(name : String)
    Database.new(self, name)
  end

  def [](name : String)
    database(name)
  end

  def list_databases(
    *,
    filter = nil,
    name_only : Bool? = nil,
    authorized_databases : Bool? = nil
  ) : Commands::ListDatabases::Result
    self.command(Commands::ListDatabases, options: {
      filter:               filter,
      name_only:            name_only,
      authorized_databases: authorized_databases,
    }).not_nil!
  end

  def server_selection(command, args, read_preference : ReadPreference) : SDAM::ServerDescription
    # See: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#multi-threaded-or-asynchronous-server-selection
    selection_start_time = Time.utc
    selection_timeout = selection_start_time + @options.server_selection_timeout

    loop do
      unless topology.compatible
        raise Mongo::Error.new topology.compatibility_error
      end

      # Find suitable servers by topology type and operation type
      # Filter the suitable servers by calling the optional, application-provided server selector.
      # If there are any suitable servers, choose one at random from those within the latency window and return it;  otherwise, continue to the next step
      suitable_servers = find_suitable_servers(command, args, read_preference)
      selected_server = suitable_servers.try { |s| select_by_latency(s) }
      return selected_server if selected_server

      # Request an immediate topology check, then block the server selection thread until the topology changes or until the server selection timeout has elapsed
      @monitors.each { |monitor|
        monitor.request_immediate_scan
      }

      select
      when @topology_update.receive
      when timeout selection_timeout - Time.utc
      end

      # If more than serverSelectionTimeoutMS milliseconds have elapsed since the selection start time, raise a server selection error
      if Time.utc > selection_timeout
        raise ServerSelectionError.new "Timeout (#{@options.server_selection_timeout}) reached while trying to select a suitable server with read preference #{read_preference.mode}."
      end
    end
  end

  def get_connection(server_description : SDAM::ServerDescription) : Mongo::Connection
    @pools[server_description] ||= DB::Pool(Mongo::Connection).new(
      initial_pool_size: @options.min_pool_size,
      max_pool_size: @options.max_pool_size,
      max_idle_pool_size: @options.min_pool_size
    ) do
      connection = Mongo::Connection.new(server_description, @credentials, @options)
      result, round_trip_time = connection.handshake(send_metadata: true)
      connection.authenticate
      new_rtt = Connection.average_round_trip_time(round_trip_time, server_description.round_trip_time)
      new_description = SDAM::ServerDescription.new(server_description.address, result, new_rtt)
      topology.update(server_description, new_description)
      connection
    end
    @pools[server_description].checkout
  rescue error : Exception
    Mongo::Log.error { "Client handshake error: #{error}" }
    # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#network-or-command-error-during-server-check
    close_connection_pool(server_description)
    description = SDAM::ServerDescription.new(server_description.address)
    description.error = error.message
    description.last_update_time = server_description.last_update_time
    topology.update(server_description, description)
    raise error
  end

  def release_connection(connection : Mongo::Connection)
    @pools[connection.server_description]?.try &.release(connection)
  end

  def close_connection_pool(server_description : SDAM::ServerDescription)
    @@lock.synchronize {
      pool = @pools.delete(server_description)
      pool.try &.close
    }
  end

  def stop_monitoring(server_description : SDAM::ServerDescription)
    @@lock.synchronize {
      @monitors.reject!(server_description)
    }
  end

  def on_topology_update
    loop do
      select
      when @topology_update.send nil
      # Fiber.yield
      else
        break
      end
    end

    @@lock.synchronize {
      self.topology.servers.each { |server|
        no_monitor = @monitors.none? { |monitor|
          monitor.server_description.address.== server.address
        }
        add_monitor(server) if no_monitor
      }
    }
  end

  def close
    @pools.each { |_, pool|
      pool.close
    }
    @monitors.each &.close
  end

  def find_suitable_servers(command, args, read_preference : ReadPreference) : Array(SDAM::ServerDescription)?
    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#topology-type-unknown
    case self.topology.type
    when .unknown?
      nil
    when .single?
      self.topology.servers
    when .replica_set_no_primary?, .replica_set_with_primary?
      if WithReadPreference.must_use_primary_command?(command, args)
        if self.topology.type.replica_set_with_primary?
          select_primary
        else
          nil
        end
      else
        if read_preference.mode == "primary"
          select_primary
        elsif read_preference.mode == "secondary" || read_preference.mode == "nearest"
          servers = select_secondaries
          if read_preference.mode == "nearest"
            servers += select_primary
          end
          servers = filter_by_staleness(servers, read_preference)
          filter_by_tags(servers, read_preference)
        elsif read_preference.mode == "secondaryPreferred"
          result = find_suitable_servers(command, args, ReadPreference.new(mode: "secondary"))
          unless result.try &.size.try &.> 0
            return select_primary
          end
          result
        elsif read_preference.mode == "primaryPreferred"
          result = select_primary
          unless result.try &.size > 0
            return find_suitable_servers(command, args, ReadPreference.new(mode: "secondary"))
          end
          result
        end
      end
    when .sharded?
      self.topology.servers.select &.type.mongos?
    end
  end

  private def select_primary
    self.topology.servers.select &.type.rs_primary?
  end

  private def select_secondaries
    self.topology.servers.select &.type.rs_secondary?
  end

  private def filter_by_staleness(server_descriptions, read_preference) : Array(SDAM::ServerDescription)?
    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#maxstalenessseconds
    max_staleness = (read_preference.max_staleness_seconds || -1).seconds
    return server_descriptions unless max_staleness >= 0.seconds
    server_descriptions.select { |server|
      next true unless server.type.rs_secondary?
      if self.topology.type.replica_set_with_primary?
        primary = select_primary[0]
        staleness = (server.last_update_time - server.last_write_date.not_nil!) - (primary.last_update_time - primary.last_write_date.not_nil!) + @options.heartbeat_frequency
      else
        max_write_date = select_secondaries.max_of &.last_write_date.not_nil!
        staleness = max_write_date - server.last_write_date.not_nil! + @options.heartbeat_frequency
      end
      staleness <= max_staleness
    }
  end

  private def filter_by_tags(server_descriptions, read_preference) : Array(SDAM::ServerDescription)?
    # https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#tag-sets
    return server_descriptions unless (tag_sets = read_preference.tags) && tag_sets.size > 0
    server_descriptions.select { |server|
      tag_sets.any? { |tags|
        tags.all? { |key, value|
          server.tags.try &.[key].== value
        }
      }
    }
  end

  private def select_by_latency(server_descriptions) : SDAM::ServerDescription?
    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#selecting-servers-within-the-latency-window
    return server_descriptions[0]? if server_descriptions.size < 2

    min_round_trip_time = server_descriptions.min_of &.round_trip_time
    eligible = server_descriptions.select { |server|
      server.round_trip_time - min_round_trip_time < @options.local_threshold
    }
    eligible.sample(1)[0]
  end

  def start_monitoring
    @monitors.each { spawn &.scan }
  end
end
