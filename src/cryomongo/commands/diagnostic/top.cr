require "bson"
require "../commands"

module Mongo::Commands::Top
  extend self

  def command
    Commands.make({
      top:   1,
      "$db": "admin",
    })
  end

  def result(bson)
    bson
  end
end
