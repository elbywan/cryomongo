require "bson"
require "../commands"

module Mongo::Commands::CloneCollectionAsCapped
  extend self

  def command(database : String, collection : Collection::CollectionKey, to_collection : Collection::CollectionKey, size : Int64, options)
    Commands.make({
      cloneCollectionAsCapped: collection,
      toCollection: to_collection,
      size: size,
      "$db": database
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
