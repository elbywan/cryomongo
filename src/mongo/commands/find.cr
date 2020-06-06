require "bson"
require "./commands"

module Mongo::Commands::Find
  extend self

  def command(database : String, collection : Collection::CollectionKey, filter, options)
    Commands.make({
      find:   collection,
      filter: BSON.new(filter),
      "$db":  database,
    }, options)
  end

  def result(bson)
    Common::QueryResult.from_bson bson
  end
end
