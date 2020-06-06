require "bson"
require "./commands"

module Mongo::Commands::Count
  extend self

  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      count: collection,
      "$db": database,
    }, options) { |key|
      key == "query" && key.nil?
    }
  end

  def result(bson)
    bson
  end
end
