require "bson"
require "../commands"

module Mongo::Commands::Drop
  extend self

  def command(database : String, name : Collection::CollectionKey, options)
    Commands.make({
      drop:  name,
      "$db": database,
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
