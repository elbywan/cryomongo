require "bson"
require "../commands"

module Mongo::Commands::RenameCollection
  extend self

  def command(database : String, collection : Collection::CollectionKey, to : String, options)
    Commands.make({
      renameCollection: collection,
      to:               to,
      "$db":            database,
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
