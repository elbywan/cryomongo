require "bson"
require "../commands"

module Mongo::Commands::CollStats
  extend self

  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      collStats: collection,
      "$db":     database,
    }, options)
  end

  def result(bson)
    bson
  end
end
