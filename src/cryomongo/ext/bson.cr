# :nodoc:
struct BSON
  # Return a copy of this BSON object having the key/value pairs overriden by those located in the named_tuple argument.
  def copy_with(named_tuple : NamedTuple) : BSON
    copy = BSON.new
    self.each { |key, value, code|
      if named_tuple[key]?
        copy[key] = named_tuple[key]
      else
        if value.is_a? BSON && code.array?
          copy.append_array(key, value)
        else
          copy[key] = value
        end
      end
    }
    named_tuple.each { |key, value|
      copy["#{key}"] = value unless copy["#{key}"]?
    }
    copy
  end
end
