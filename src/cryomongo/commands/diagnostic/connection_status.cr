require "bson"
require "../commands"

module Mongo::Commands::ConnectionStatus
  extend self

  def command(options)
    Commands.make({
      connectionStatus: 1,
      "$db":            "admin",
    }, options)
  end

  def result(bson)
    bson
  end
end
