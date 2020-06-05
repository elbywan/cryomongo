require "bson"
require "./commands"

module Mongo::Commands::FindAndModify
  extend self

  def command(database : String, collection : Collection::CollectionKey, filter, options)
    Commands.make({
      findAndModify: collection,
      query: BSON.new(filter),
      "$db": database
    }, options)
  end

  def result(bson)
    Common::QueryResult.from_bson bson
  end
end
