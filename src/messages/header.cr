require "./op_code"

struct Mongo::Messages::Header
  # total message size, including this
  getter message_length : Int32
  #  identifier for this message
  getter request_id : Int32
  # requestID from the original request (used in responses from db)
  getter response_to : Int32 = 0
  # request type
  getter op_code : OpCode

  def initialize(@message_length, @request_id, @op_code, @response_to = 0)
  end

  def initialize(io : IO)
    @message_length = Int32.from_io(io, IO::ByteFormat::LittleEndian)
    @request_id = Int32.from_io(io, IO::ByteFormat::LittleEndian)
    @response_to = Int32.from_io(io, IO::ByteFormat::LittleEndian)
    @op_code = OpCode.from_value Int32.from_io(io, IO::ByteFormat::LittleEndian)
  end

  def to_io(io : IO)
    @message_length.to_io(io, IO::ByteFormat::LittleEndian)
    @request_id.to_io(io, IO::ByteFormat::LittleEndian)
    @response_to.to_io(io, IO::ByteFormat::LittleEndian)
    @op_code.value.to_io(io, IO::ByteFormat::LittleEndian)
  end

  def body_size
    message_length - 16
  end
end
