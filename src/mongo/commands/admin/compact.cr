require "bson"
require "../commands"

module Mongo::Commands::Compact
  extend self

  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      compact: collection,
      "$db": database
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
