require "bson"
require "./commands"

module Mongo::Commands::Distinct
  extend self

  def command(database : String, collection : Collection::CollectionKey, key : String, options)
    Commands.make({
      distinct: collection,
      key:      key,
      "$db":    database,
    }, options)
  end

  Common.result(Result) {
    property values : BSON
  }

  def result(bson)
    Result.from_bson bson
  end
end
