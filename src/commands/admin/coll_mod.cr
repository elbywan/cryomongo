require "bson"
require "../commands"

module Mongo::Commands::CollMod
  extend self

  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      collMod: collection,
      "$db": database
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
