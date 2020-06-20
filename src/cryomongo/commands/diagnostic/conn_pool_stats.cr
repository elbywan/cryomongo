require "bson"
require "../commands"

module Mongo::Commands::ConnPoolStats
  extend self

  def command
    Commands.make({
      connPoolStats: 1,
      "$db":         "admin",
    }, options)
  end

  def result(bson)
    bson
  end
end
