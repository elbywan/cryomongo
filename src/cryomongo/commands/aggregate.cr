require "bson"
require "./commands"

module Mongo::Commands::Aggregate
  extend self

  def command(database : String, collection : Collection::CollectionKey, pipeline : Array, options)
    need_cursor = true
    body, sequences = Commands.make({
      "aggregate": collection,
      "pipeline":  pipeline.map { |elt| BSON.new(elt) },
      "$db":       database,
    }, options) { |_, key, value|
      need_cursor = false if (key.to_s == "explain" || key.to_s == "cursor") && !value.nil?
      false
    }
    body["cursor"] = BSON.new if need_cursor
    {body, sequences}
  end

  def result(bson)
    raise Mongo::Error.new "Explain is not supported" unless bson["cursor"]?
    Common::QueryResult.from_bson bson
  end
end
