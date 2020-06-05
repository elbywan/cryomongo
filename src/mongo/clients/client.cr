require "socket"
require "../database"
require "../messages/**"
require "../commands/**"
require "../error"
require "../concerns"
require "../read_preference"
require "../sdam/topology_description"

abstract class Mongo::Client
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  MIN_WIRE_VERSION = 6
  MAX_WIRE_VERSION = 8

  abstract def topology : SDAM::TopologyDescription
  abstract def options : Options
  abstract def server_selection(command, args, read_preference : ReadPreference) : SDAM::ServerDescription
  abstract def get_connection(server_description : SDAM::ServerDescription) : IO
  abstract def release_connection(server_description : SDAM::ServerDescription, socket : IO)
  abstract def on_topology_update : Void

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
      read_preference ||= ReadPreference.new(mode: "primary")
    end

    server_description ||= server_selection(cmd, args, read_preference)
    socket = get_connection(server_description)

    args = WithReadPreference.mix_read_preference(cmd, args, read_preference, topology, server_description)

    unacknowledged = false
    if concern = args["options"]?.try(&.["write_concern"]?)
      unacknowledged = concern.as(WriteConcern).w == 0 && concern.as(WriteConcern).j == false
    end

    body, sequences = cmd.command(**args)
    flag_bits = unacknowledged ? Messages::OpMsg::Flags::MoreToCome : Messages::OpMsg::Flags::None
    op_msg = Messages::OpMsg.new(
      flag_bits: flag_bits,
      sections: [
        Messages::OpMsg::SectionBody.new(body)
      ].map(&.as(Messages::Part))
    )
    sequences.try &.each { |key, documents|
      op_msg.sequence(key.to_s, contents: documents)
    }

    send_op_msg(socket, op_msg)

    return nil if unacknowledged

    op_msg = receive_op_msg(socket, ignore_errors: ignore_errors)

    result = cmd.result(op_msg.body)

    if result.is_a? Cursor
      result.server_description = server_description
    end

    result
  ensure
    release_connection(server_description, socket) if socket && server_description
  end

  def send_op_msg(socket : IO, op_msg : Messages::OpMsg)
    message =  Messages::Message.new(op_msg)
    Log.verbose {
      "Sending: #{message.header.inspect}"
    }
    Log.debug {
      op_msg.body.to_json
    }
    op_msg.each_sequence { |key, contents|
      Log.debug {
        "Seq[#{key}]: #{contents.to_json}"
      }
    }
    message.to_io(socket)
  end

  def receive_op_msg(socket : IO, ignore_errors = false)
    loop do
      message = Mongo::Messages::Message.new(socket)
      Log.verbose {
        "Receiving: #{message.header.inspect}"
      }
      op_msg = message.contents.as(Messages::OpMsg)
      Log.debug {
        op_msg.body.to_json
      }
      op_msg.each_sequence { |key, contents|
        Log.debug {
          "Seq[#{key}]: #{contents.to_json}"
        }
      }
      unless op_msg.body["ok"] == 1
        err_msg = op_msg.body["errmsg"]?.as(String)
        err_code = op_msg.body["code"]?
        Log.error {
          "Received error code: #{err_code} - #{err_msg}"
        }
        raise Mongo::CommandError.new(err_code, err_msg) unless ignore_errors
      end
      return op_msg unless op_msg.flag_bits.more_to_come?
    end
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
      filter: filter,
      name_only: name_only,
      authorized_databases: authorized_databases
    }).not_nil!
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
    return server_descriptions unless max_staleness = (read_preference.max_staleness_seconds || 0).seconds
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
    return server_descriptions unless tag_sets = read_preference.tags
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
end
