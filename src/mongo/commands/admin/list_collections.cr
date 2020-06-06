require "bson"
require "../commands"

module Mongo::Commands::ListCollections
  extend self

  def command(database : String, options)
    Commands.make({
      listCollections: 1,
      "$db":           database,
    }, options)
  end

  def result(bson)
    Common::QueryResult.from_bson bson
  end
end
