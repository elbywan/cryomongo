require "bson"
require "./commands"

module Mongo::Commands::Delete
  extend self

  def command(database : String, collection : Collection::CollectionKey, deletes : Array, options)
    Commands.make({
      delete: collection,
      "$db": database
    }, sequences: {
      deletes: deletes.map { |elt| BSON.new(elt) }
    }, options: options)
  end

  def result(bson)
    Common::DeleteResult.from_bson bson
  end
end
