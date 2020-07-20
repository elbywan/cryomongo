require "bson"
require "../commands"

# Builds one or more indexes on a collection.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/createIndexes/).
module Mongo::Commands::CreateIndexes
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, indexes : Array, options)
    Commands.make({
      createIndexes: collection,
      indexes:       indexes.map { |index| BSON.new(index) },
      "$db":         database,
    }, options)
  end

  Common.result(Result) {
    property created_collection_automatically : Bool?
    property num_indexes_before : Int32?
    property num_indexes_after : Int32?
    property note : String?
    property code : Int32?
    property errmsg : String?
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
