require "bson"
require "../commands"

module Mongo::Commands::DropIndexes
  extend self

  def command(database : String, collection : Collection::CollectionKey, index, options)
    Commands.make({
      dropIndexes: collection,
      index: index,
      "$db": database
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
