require "bson"
require "../commands"

module Mongo::Commands::DbStats
  extend self

  def command(database : String, options)
    Commands.make({
      dbStats: 1,
      "$db":     database,
    }, options)
  end

  def result(bson)
    bson
  end
end
