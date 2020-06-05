require "socket"

abstract class Mongo::SDAM::Monitor

  @heartbeat_frequency : Time::Span = 60.seconds
  @cooldown : Time::Span = 500.milliseconds
  @topology : TopologyDescription

  def initialize(@topology : TopologyDescription, @heartbeat_frequency  : Time::Span = 60.seconds)
  end

  abstract def get_connection(server_description : ServerDescription)
  abstract def close_connection(server_description : ServerDescription)
  abstract def scan

  def check(server_description : ServerDescription)
    server_description.last_update_time = Time.utc
    socket = get_connection(server_description)

    body, _ = Commands::IsMaster.command
    op_msg = Messages::OpMsg.new(
      flag_bits: :none,
      sections: [
        Messages::OpMsg::SectionBody.new(body)
      ].map(&.as(Messages::Part))
    )
    full_msg =  Messages::Message.new(op_msg)

    reply = uninitialized Mongo::Messages::Message
    round_trip_time = Time.measure {
      full_msg.to_io(socket)
      reply = Mongo::Messages::Message.new(socket)
    }
    op_msg = reply.contents.as(Messages::OpMsg)
    unless op_msg.body["ok"]? == 1
      raise Mongo::CommandError.new(op_msg.body["code"]?, op_msg.body["errmsg"]?)
    end
    result = Commands::IsMaster.result(op_msg.body)

    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#calculation-of-average-round-trip-times
    if server_description.round_trip_time
      alpha = 0.2
      new_rtt = (0.2 * round_trip_time.milliseconds + (1 - alpha) * server_description.round_trip_time.milliseconds).milliseconds
    else
      new_rtt = round_trip_time
    end

    ServerDescription.new(server_description.address, result, new_rtt)

    rescue error : Exception
      # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#network-or-command-error-during-server-check
      close_connection(server_description)
      known_state = !server_description.type.unknown?
      description = ServerDescription.new(server_description.address)
      description.error = error.message
      description.last_update_time = server_description.last_update_time
      if known_state && error.is_a? Socket::Error
        check(description)
      else
        description
      end
  end
end
