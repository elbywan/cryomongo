require "./error"

module Mongo::Tools
  extend self

  def merge_bson(init, options = nil, skip_nil = true, &block)
    bson = BSON.new(init)
    options.try &.each { |key, value|
      skip_key = yield bson, key, value
      if skip_key == false && (skip_nil == false || !value.nil?)
        bson[key.to_s.camelcase(lower: true)] = value
      end
    }
    bson
  end

  def merge_bson(init, options = nil, skip_nil = true)
    self.merge_bson(init, options, skip_nil) { false }
  end

  module Initializer
    macro included
      {% verbatim do %}
      def self.new(**args)
        instance = self.allocate
        {% for ivar in @type.instance_vars %}
          {% default_value = ivar.default_value %}
          {% if ivar.type.nilable? %}
            instance.{{ivar.id}} = args["{{ivar.id}}"]? {% if ivar.has_default_value? %}|| {{ default_value }}{% end %}
          {% else %}
            if value = args["{{ivar.id}}"]?
              instance.{{ivar.id}} = value
            {% if ivar.has_default_value? %}
            else
              instance.{{ivar.id}} = {{ default_value }}
            {% end %}
            end
          {% end %}
        {% end %}
        instance
      end
      {% end %}
    end
  end
end
