require "bson"
require "../commands"

module Mongo::Commands::Validate
  extend self

  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      validate: collection,
      "$db": database,
    }, options)
  end

  def result(bson)
    bson
  end
end
