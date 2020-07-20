require "bson"
require "../commands"

# Performs aggregation operation using the aggregation pipeline.
# The pipeline allows users to process data from a collection or other source with a sequence of stage-based manipulations.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/aggregate/).
module Mongo::Commands::Aggregate
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
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

  # Transforms the server result.
  def result(bson : BSON)
    raise Mongo::Error.new "Explain is not supported" unless bson["cursor"]?
    Common::QueryResult.from_bson bson
  end
end
