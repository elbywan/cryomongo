require "bson"
require "../commands"

module Mongo::Commands::ReIndex
  extend self

  def command(database : String, collection : Collection::CollectionKey)
    Commands.make({
      reIndex: collection,
      "$db": database
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
