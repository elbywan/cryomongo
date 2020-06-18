require "bson"
require "../commands"

module Mongo::Commands::GetCmdLineOpts
  extend self

  def command
    Commands.make({
      getCmdLineOpts: 1,
      "$db":    "admin",
    })
  end

  def result(bson)
    bson
  end
end
