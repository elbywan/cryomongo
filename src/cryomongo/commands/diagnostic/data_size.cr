require "bson"
require "../commands"

module Mongo::Commands::DataSize
  extend self

  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      dataSize: collection,
      "$db":    database,
    }, options)
  end

  def result(bson)
    bson
  end
end
