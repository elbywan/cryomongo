require "bson"
require "../commands"

# *getLog* is an administrative command that returns the most recent 1024 logged mongod events.
#
# *getLog* does not read log data from the mongod log file.
# It instead reads data from a RAM cache of logged mongod events.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/getLog/).
module Mongo::Commands::GetLog
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(log_value : String)
    Commands.make({
      getLog: log_value,
      "$db":  "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
