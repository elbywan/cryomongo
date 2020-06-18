require "bson"
require "../commands"

module Mongo::Commands::DbHash
  extend self

  def command(database : String, options)
    Commands.make({
      dbHash: 1,
      "$db":  database,
    }, options)
  end

  def result(bson)
    bson
  end
end
