require "bson"
require "../commands"

module Mongo::Commands::HostInfo
  extend self

  def command
    Commands.make({
      hostInfo: 1,
      "$db":    "admin",
    })
  end

  def result(bson)
    bson
  end
end
