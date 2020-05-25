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
end
