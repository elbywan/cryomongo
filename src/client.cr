require "socket"
require "./database"
require "./messages/**"
require "./commands/**"
require "./error"
require "./concerns"

class Mongo::Client
  include WithReadConcern
  include WithWriteConcern

  @socket : TCPSocket
  getter handshake_reply : Commands::IsMaster::Result

  def initialize(address : String = "mongodb://localhost:27017")
    uri = URI.parse(address)
    @socket = TCPSocket.new(uri.host.not_nil!, uri.port)
    @handshake_reply = command(Commands::IsMaster).not_nil!
  end

  def command(operation, ignore_errors = false, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, **args)
    args = WithWriteConcern.mix_write_concern(operation, args, write_concern)
    args = WithReadConcern.mix_read_concern(operation, args, read_concern)

    unacknowledged = false
    if concern = args["options"]?.try(&.["write_concern"]?)
      unacknowledged = concern.as(WriteConcern).w == 0 && concern.as(WriteConcern).j == false
    end

    body, sequences = operation.command(**args)
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

    send_op_msg(op_msg)

    return nil if unacknowledged

    op_msg = receive_op_msg(ignore_errors: ignore_errors)

    operation.result(op_msg.body)
  end

  def send_op_msg(op_msg : Messages::OpMsg)
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
    message.to_io(@socket)
  end

  def receive_op_msg(ignore_errors = false)
    loop do
      message = Mongo::Messages::Message.new(@socket)
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
        raise Mongo::Error.new(err_code, err_msg) unless ignore_errors
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

  def close
    @socket.close
  end

  def finalize
    @socket.close unless @socket.closed?
  end

  def list_databases(**options) : Commands::ListDatabases::Result
    self.command(Commands::ListDatabases, options: options).not_nil!
  end
end
