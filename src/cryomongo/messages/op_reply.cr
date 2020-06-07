require "bson"
require "./message_part"
require "./op_code"

# The OP_REPLY message is sent by the database in response to an OP_QUERY or OP_GET_MORE message.
struct Mongo::Messages::OpReply < Mongo::Messages::Part
  @[Field(ignore: true)]
  @op_code : OpCode = OpCode::Reply

  @[Flags]
  enum ResponseFlags : Int32
    # Is set when getMore is called but the cursor id is not valid at the server. Returned with zero results.
    CursorNotFound
    # Is set when query failed. Results consist of one document containing an “$err” field describing the failure.
    QueryFailure
    # Drivers should ignore this. Only mongos will ever see this set, in which case, it needs to update config from the server.
    ShardConfigStale
    # Is set when the server supports the AwaitData Query option.
    # If it doesn’t, a client should sleep a little between getMore’s of a Tailable cursor.
    # Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.
    AwaitCapable
  end

  # Bit vector to specify flags.
  getter response_flags : ResponseFlags
  # The cursorID that this OP_REPLY is a part of.
  # In the event that the result set of the query fits into one OP_REPLY message, cursorID will be 0.
  # This cursorID must be used in any OP_GET_MORE messages used to get more data,
  # and also must be closed by the client when no longer needed via a OP_KILL_CURSORS message.
  getter cursor_id : Int64
  # Starting position in the cursor.
  getter starting_from : Int32
  # Number of documents in the reply.
  getter number_returned : Int32
  # Returned documents.
  getter documents : Array(BSON)

  def initialize(
    @response_flags,
    @cursor_id,
    @starting_from,
    @number_returned,
    @documents
  )
  end

  def initialize(io : IO, header : Messages::Header)
    size = header.body_size
    msg_bytes = Bytes.new(size)
    io.read_fully(msg_bytes)
    msg_view = IO::Memory.new(msg_bytes)
    @response_flags = ResponseFlags.from_value Int32.from_io(msg_view, IO::ByteFormat::LittleEndian)
    @cursor_id = Int64.from_io(msg_view, IO::ByteFormat::LittleEndian)
    @starting_from = Int32.from_io(msg_view, IO::ByteFormat::LittleEndian)
    @number_returned = Int32.from_io(msg_view, IO::ByteFormat::LittleEndian)
    @documents = [] of BSON
    loop do
      break if msg_view.pos >= size
      @documents << BSON.new msg_view
    end
  end
end
