require "bson"
require "../commands"

module Mongo::Commands::GetLog
  extend self

  def command(log_value : String)
    Commands.make({
      getLog: log_value,
      "$db":  "admin",
    })
  end

  def result(bson)
    bson
  end
end
