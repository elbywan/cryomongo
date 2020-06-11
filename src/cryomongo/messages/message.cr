require "./header"
require "./message_part"

struct Mongo::Messages::Message
  @@id : Atomic(Int32) = Atomic(Int32).new(0)

  getter header : Header
  getter contents : Part

  def initialize(contents : Part, response_to = 0)
    @header = Header.new(
      message_length: 16 + contents.part_size,
      request_id: @@id.add(1),
      response_to: response_to,
      op_code: contents.op_code
    )
    @contents = contents
  end

  def initialize(io : IO)
    @header = Mongo::Messages::Header.new(io)
    case header.op_code
    when .reply?
      @contents = Mongo::Messages::OpReply.new(io, header)
    when .msg?
      @contents = Mongo::Messages::OpMsg.new(io: io, header: header)
    else
      raise Mongo::Error.new "Received unexpected message op_code: #{header.op_code}"
    end
  end

  def to_io(io : IO)
    @header.to_io io
    @contents.to_io io
    io.flush
  end
end
