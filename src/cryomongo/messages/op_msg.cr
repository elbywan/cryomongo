require "bson"
require "./message_part"
require "./op_code"

# OP_MSG is an extensible message format designed to subsume the functionality of other opcodes.
struct Mongo::Messages::OpMsg < Mongo::Messages::Part
  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::Msg

  @[Flags]
  enum Flags : UInt32
    # The message ends with 4 bytes containing a CRC-32C checksum.
    ChecksumPresent
    # Another message will follow this one without further action from the receiver.
    # The receiver MUST NOT send another message until receiving one with moreToCome set to 0 as sends may block, causing deadlock.
    # Requests with the moreToCome bit set will not receive a reply. Replies will only have this set in response to requests with the exhaustAllowed bit set.
    MoreToCome
    # The client is prepared for multiple replies to this request using the moreToCome bit.
    # The server will never produce replies with the moreToCome bit set unless the request has this bit set.
    ExhaustAllowed = 16
  end

  getter flag_bits : Flags
  getter sections : Array(Part)
  getter checksum : UInt32?

  def initialize(@flag_bits : Flags, @sections, @checksum = nil)
  end

  def initialize(body, *, flag_bits : Flags = :none)
    initialize(
      flag_bits: flag_bits,
      sections: [
        Messages::OpMsg::SectionBody.new(BSON.new(body)),
      ].map(&.as(Messages::Part))
    )
  end

  def initialize(io : IO, header : Messages::Header)
    size = header.body_size
    msg_bytes = Bytes.new(size)
    io.read_fully(msg_bytes)
    msg_view = IO::Memory.new(msg_bytes)

    @flag_bits = Flags.from_value UInt32.from_io(msg_view, IO::ByteFormat::LittleEndian)
    @sections = typeof(@sections).new

    has_checksum = @flag_bits.checksum_present?

    loop do
      break if msg_view.pos >= size - (has_checksum ? 4 : 0)
      payload_type = UInt8.from_io(msg_view, IO::ByteFormat::LittleEndian)
      case payload_type
      when 0_u8
        payload = BSON.new msg_view
        @sections << SectionBody.new(payload)
      when 1_u8
        marker = msg_view.pos
        sequence_size = Int32.from_io(msg_view, IO::ByteFormat::LittleEndian)
        delimited = IO::Delimited.new(msg_view, read_delimiter: "\0")
        sequence_identifier = delimited.gets_to_end
        contents = Array(BSON).new
        loop do
          break if msg_view.pos - marker >= sequence_size
          contents << BSON.new(msg_view)
        end
        @sections << SectionDocumentSequence.new(
          payload: SectionDocumentSequence::SectionPayload.new(
            sequence_identifier, contents
          )
        )
      else
        raise Mongo::Error.new "Received invalid payload type: #{payload_type}"
      end
    end

    if has_checksum
      @checksum = UInt32.from_io(msg_view, IO::ByteFormat::LittleEndian)
    end
  end

  struct SectionBody < Part
    getter payload_type : UInt8 = 0_u8
    getter payload : BSON

    def initialize(@payload : BSON); end
  end

  struct SectionDocumentSequence < Part
    getter payload_type : UInt8 = 1_u8
    getter payload : SectionPayload

    def initialize(@payload : SectionPayload); end

    struct SectionPayload < Part
      # Payload size (includes this 4-byte field).
      getter sequence_size : Int32 = 0
      # Document sequence identifier. In all current commands this field is the (possibly nested) field that it is replacing from the body section.
      # This field MUST NOT also exist in the body section.
      getter sequence_identifier : String
      # Zero or more BSON objects.
      getter contents : Array(BSON)

      def initialize(@sequence_identifier, @contents)
        @sequence_size = self.part_size
      end
    end
  end

  def body : BSON
    sections.find(&.is_a? SectionBody).not_nil!.as(SectionBody).payload
  end

  def each_sequence
    sections.each { |section|
      if section.is_a? SectionDocumentSequence
        yield section.payload.sequence_identifier, section.payload.contents
      end
    }
  end

  def sequence(key : String, contents : Array(BSON))
    @sections << SectionDocumentSequence.new(
      payload: SectionDocumentSequence::SectionPayload.new(
        sequence_identifier: key,
        contents: contents
      )
    )
  end

  def valid?
    self.body["ok"] == 1
  end

  def validate : Exception?
    if self.valid?
      if errors = self.body["writeErrors"]?
        Mongo::Error::CommandWrite.new(errors.as(BSON))
      end
    else
      err_msg = self.body["errmsg"]?.as(String)
      err_code = self.body["code"]?
      Mongo::Error::Command.new(err_code, err_msg)
    end
  end

  def safe_payload(command)
    # see: https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst#security
    if command.is_a?(Commands::IsMaster) && self.body["speculativeAuthenticate"]?
      BSON.new
    else
      payload = BSON.new(self.body)
      self.each_sequence { |key, contents|
        payload[key] = contents
      }
      payload
    end
  end
end
