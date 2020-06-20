require "bson"
require "../commands"

module Mongo::Commands::ListCommands
  extend self

  def command
    Commands.make({
      listCommands: 1,
      "$db":        "admin",
    })
  end

  def result(bson)
    bson
  end
end
