require "bson"
require "../commands"

module Mongo::Commands::ConvertToCapped
  extend self

  def command(database : String, collection : Collection::CollectionKey, size : Int64, options)
    Commands.make({
      convertToCapped: collection,
      size: size,
      "$db": database
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
