require "bson"
require "../commands"

module Mongo::Commands::ListIndexes
  extend self

  def command(database : String, collection : Collection::CollectionKey)
    Commands.make({
      listIndexes: collection,
      "$db": database
    })
  end

  def result(bson)
    Common::QueryResult.from_bson bson
  end
end
