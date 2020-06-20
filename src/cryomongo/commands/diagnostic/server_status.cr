require "bson"
require "../commands"

module Mongo::Commands::ServerStatus
  extend self

  def command(options)
    Commands.make({
      serverStatus: 1,
      "$db":        "admin",
    }, options)
  end

  def result(bson)
    bson
  end
end
