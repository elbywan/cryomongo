module Crud::Helpers
  macro included
  {% verbatim do %}
    private macro bson_arg(name)
      arguments["{{name.id}}"]?.try { |%arg|
        BSON.from_json(%arg.to_json)
      }
    end

    private macro bson_array_arg(name)
      arguments["{{name.id}}"]?.try { |%arg|
        %arg.as_a.map{ |elt|
          BSON.from_json(elt.to_json)
        }
      }
    end

    private macro int32_arg(name)
      arguments["{{name.id}}"]?.try &.as_i
    end

    private macro int64_arg(name)
      arguments["{{name.id}}"]?.try &.as_i64
    end

    private macro string_arg(name)
      arguments["{{name.id}}"]?.try &.as_s
    end

    private macro bool_arg(name)
      arguments["{{name.id}}"]?.try &.as_bool
    end

    private def compare_json(a : JSON::Any, b : JSON::Any)
      if a.as_a?
        a.as_a.each_with_index { |elt, index|
        compare_json(elt, b[index])
        }
      elsif a.as_h?
        a.as_h.each { |k, v|
          compare_json(v, b.as_h[k])
        }
      else
        a.should eq b
      end
    end
  {% end %}
  end
end
