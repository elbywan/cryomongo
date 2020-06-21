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

# The client which provides access to a MongoDB server, replica set, or sharded cluster.
#
# It maintains management of underlying sockets and routing to individual nodes.
class Mongo::Client
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  # The mininum wire protocol version supported by this driver.
  MIN_WIRE_VERSION = 6
  # The maximum wire protocol version supported by this driver.
  MAX_WIRE_VERSION = 8

  # :nodoc:
  UNACKNOWLEDGED_WRITE_PROHIBITED_OPTIONS = {
    "hint",
    "collation",
    "bypass_document_validation",
    "array_filters",
  }

  # :nodoc:
  getter! topology : SDAM::TopologyDescription
  # The set of driver options.
  getter options : Options

  @@lock = Mutex.new(:reentrant)
  @pools : Hash(SDAM::ServerDescription, DB::Pool(Mongo::Connection)) = Hash(SDAM::ServerDescription, DB::Pool(Mongo::Connection)).new
  @monitors : Array(SDAM::Monitor) = Array(SDAM::Monitor).new
  @socket_check_interval : Time::Span = 5.seconds
  @last_scan : Time = Time::UNIX_EPOCH
  @min_heartbeat_frequency : Time::Span = 500.milliseconds
  @topology_update = Channel(Nil).new

  # Create a mongodb client instance from a mongodb URL.
  #
  # ```
  # require "cryomongo"
  #
  # client = Mongo::Client.new "mongodb://127.0.0.1/?appname=client-example"
  # ```
  def initialize(connection_string : String = "mongodb://localhost:27017", options : Mongo::Options = Mongo::Options.new)
    initialize(connection_string: connection_string, options: options, start_monitoring: true)
  end

  # :nodoc:
  def initialize(connection_string : String = "mongodb://localhost:27017", *, options : Mongo::Options = Mongo::Options.new, start_monitoring = true)
    seeds, @options, @credentials = Mongo::URI.parse(connection_string, options)

    if (w = @options.w) || (w_timeout = @options.w_timeout) || (journal = @options.journal)
      @write_concern = WriteConcern.new(w: w, w_timeout: w_timeout.try &.milliseconds.to_i64, j: journal)
    end

    if read_concern_level = @options.read_concern_level
      @read_concern = ReadConcern.new(level: read_concern_level)
    end

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

  # Frees all the resources associated with a client.
  def close
    @pools.each { |_, pool|
      pool.close
    }
    @monitors.each &.close
  end

  ##################
  # Public Methods #
  ##################

  # Get a newly allocated `Mongo::Database` for the database named *name*.
  def database(name : String) : Database
    Database.new(self, name)
  end

  # :ditto:
  def [](name : String) : Database
    database(name)
  end

  # Execute a command on the server.
  #
  # ```
  # # First argument is the `Mongo::Commands`.
  # client.command(Mongo::Commands::DropDatabase, database: "database_name")
  # ```
  def command(
    command cmd,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    server_description : SDAM::ServerDescription? = nil,
    ignore_errors = false,
    **args
  )
    args = WithWriteConcern.mix_write_concern(cmd, args, write_concern || @write_concern)
    args = WithReadConcern.mix_read_concern(cmd, args, read_concern || @read_concern)

    if WithReadPreference.must_use_primary_command?(cmd, args)
      read_preference = ReadPreference.new(mode: "primary")
    else
      read_preference = read_preference || @read_preference || ReadPreference.new(mode: "primary")
    end

    server_description ||= server_selection(cmd, args, read_preference)
    connection = get_connection(server_description)

    if cmd == Mongo::Commands::FindAndModify && args["options"]?.try(&.["hint"]?) && server_description.max_wire_version < 8
      raise Mongo::Error.new "The hint option is not supported by MongoDB servers < 4.2"
    end

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
  rescue error : IO::Error
    Mongo::Log.error { "Client error: #{error}" }
    # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#network-or-command-error-during-server-check
    server_description.try { |desc|
      description = SDAM::ServerDescription.new(desc.address)
      description.error = error.message
      description.last_update_time = desc.last_update_time
      topology.update(desc, description)
      close_connection_pool(desc)
    }
    raise error
  rescue error : Mongo::Error::Command
    Mongo::Log.error { "Server error: #{error}" }
    # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering
    if error.state_change?
      server_description.try { |desc|
        description = SDAM::ServerDescription.new(desc.address)
        description.error = error.message
        description.last_update_time = desc.last_update_time
        topology.update(desc, description)
        close_connection_pool(desc) if error.shutdown?
        @monitors.find(&.server_description.address.== desc.address).try &.request_immediate_scan
      }
    end
    raise error
  ensure
    release_connection(connection) if connection
  end

  # Provides a list of all existing databases along with basic statistics about them.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listDatabases).
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

  # Returns a document that provides an overview of the database’s state.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/serverStatus/).
  def status(*, repl : Int32? = nil, metrics : Int32? = nil, locks : Int32? = nil, mirrored_reads : Int32? = nil, latch_analysis : Int32? = nil) : BSON?
    self.command(Commands::ServerStatus, options: {
      repl:           repl,
      metrics:        metrics,
      locks:          locks,
      mirrored_reads: mirrored_reads,
      latch_analysis: latch_analysis,
    })
  end

  # An administrative command that returns usage statistics for each collection.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/top).
  def top : BSON?
    self.command(Commands::Top)
  end

  # Allows a client to observe all changes in a cluster.
  #
  # Returns a change stream on all collections in all databases in a cluster.
  #
  # NOTE: Excludes system collections.
  def watch(
    pipeline : Array = [] of BSON,
    *,
    full_document : String? = nil,
    resume_after = nil,
    max_await_time_ms : Int64? = nil,
    batch_size : Int32? = nil,
    collation : Collation? = nil,
    start_at_operation_time : Time? = nil,
    start_after = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil
  ) : Mongo::ChangeStream::Cursor
    ChangeStream::Cursor.new(
      client: self,
      database: "admin",
      collection: 1,
      pipeline: pipeline.map { |elt| BSON.new(elt) },
      full_document: full_document,
      resume_after: resume_after,
      start_after: start_after,
      start_at_operation_time: start_at_operation_time,
      read_concern: read_concern,
      read_preference: read_preference,
      max_time_ms: max_await_time_ms,
      batch_size: batch_size,
      collation: collation,
    )
  end

  ############
  # Internal #
  ############

  private def server_selection(command, args, read_preference : ReadPreference) : SDAM::ServerDescription
    # See: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#multi-threaded-or-asynchronous-server-selection
    selection_start_time = Time.utc
    selection_timeout = selection_start_time + @options.server_selection_timeout

    loop do
      unless topology.compatible
        raise Error::ServerSelection.new topology.compatibility_error
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
        raise Error::ServerSelection.new "Timeout (#{@options.server_selection_timeout}) reached while trying to select a suitable server with read preference #{read_preference.mode}."
      end
    end
  end

  protected def get_connection(server_description : SDAM::ServerDescription) : Mongo::Connection
    @pools[server_description] ||= DB::Pool(Mongo::Connection).new(
      initial_pool_size: @options.min_pool_size,
      max_pool_size: @options.max_pool_size,
      max_idle_pool_size: @options.min_pool_size,
      checkout_timeout: @options.wait_queue_timeout.try(&.milliseconds.to_f64) || 5.0
    ) do
      connection = Mongo::Connection.new(server_description, @credentials, @options)
      result, round_trip_time = connection.handshake(send_metadata: true, appname: @options.appname)
      connection.authenticate
      new_rtt = Connection.average_round_trip_time(round_trip_time, server_description.round_trip_time)
      new_description = SDAM::ServerDescription.new(server_description.address, result, new_rtt)
      topology.update(server_description, new_description)
      connection
    end
    @pools[server_description].checkout
  end

  private def release_connection(connection : Mongo::Connection)
    @pools[connection.server_description]?.try &.release(connection)
  end

  protected def close_connection_pool(server_description : SDAM::ServerDescription)
    @@lock.synchronize {
      pool = @pools.delete(server_description)
      pool.try &.close
    }
  end

  protected def stop_monitoring(server_description : SDAM::ServerDescription)
    @@lock.synchronize {
      @monitors.reject!(server_description)
    }
  end

  protected def on_topology_update
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

  private def find_suitable_servers(command, args, read_preference : ReadPreference) : Array(SDAM::ServerDescription)?
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

  private def start_monitoring
    @monitors.each { spawn &.scan }
  end

  protected def add_monitor(server_description : SDAM::ServerDescription, *, start_monitoring = true)
    monitor = SDAM::Monitor.new(self, server_description, @credentials, @options.heartbeat_frequency || 10.seconds)
    @monitors << monitor
    if start_monitoring
      spawn monitor.scan
    end
  end
end
