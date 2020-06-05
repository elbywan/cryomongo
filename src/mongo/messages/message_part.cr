abstract struct Mongo::Messages::Part
  annotation Field; end

  def self.field_size(field)
    case field
    when Number
      sizeof(typeof(field))
    when String
      field.bytesize + 1
    when BSON
      field.size
    when Array
      field.sum { |elt|
        self.field_size elt
      }
    when Enum
      sizeof(typeof(field.value))
    when Messages::Part
      field.part_size
    else raise "Unsupported field type: #{typeof(field)}."
    end
  end

  def self.field_to_io(io : IO, field)
    case field
    when Number
      field.to_io(io, IO::ByteFormat::LittleEndian)
    when String
      io << field
      io.write_byte 0_u8
    when BSON
      io.write field.data
    when Array
      field.each { |elt|
        self.field_to_io io, elt
      }
    when Enum
      field.value.to_io(io, IO::ByteFormat::LittleEndian)
    when Messages::Part
      field.to_io(io)
    else raise "Unsupported field type: #{typeof(field)}."
    end
  end

  def part_size
    s = 0
    {% begin %}
      {% for ivar in @type.instance_vars %}
        {% ann = ivar.annotation( Mongo::Messages::Part::Field) %}
        {% unless ann && ann[:ignore] %}
          s += Messages::Part.field_size({{ivar.id}}) if {{ivar.id}}
        {% end %}
      {% end %}
    {% end %}
    s
  end

  def to_io(io : IO)
    {% begin %}
      {% for ivar in @type.instance_vars %}
        {% ann = ivar.annotation( Mongo::Messages::Part::Field) %}
        {% unless ann && ann[:ignore] %}
          Messages::Part.field_to_io(io, {{ivar.id}}) if {{ivar.id}}
        {% end %}
      {% end %}
    {% end %}
  end
end
