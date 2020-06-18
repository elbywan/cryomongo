require "bson"
require "../commands"

module Mongo::Commands::CreateIndexes
  extend self

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

  def result(bson)
    Result.from_bson bson
  end
end
