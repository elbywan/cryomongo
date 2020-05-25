require "bson"
require "./commands"

module Mongo::Commands::Insert
  extend self

  def command(database : String, collection : Collection::CollectionKey, documents : Array, options)
    Commands.make({
      insert: collection,
      "$db": database
    }, sequences: {
      documents: documents.map { |elt| BSON.new(elt) },
    }, options: options)
  end

  def result(bson)
    Common::InsertResult.from_bson bson
  end
end
