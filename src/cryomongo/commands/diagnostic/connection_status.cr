require "bson"
require "../commands"

# Returns information about the current connection, specifically the state of authenticated users and their available permissions.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/connectionStatus/).
module Mongo::Commands::ConnectionStatus
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options = nil)
    Commands.make({
      connectionStatus: 1,
      "$db":            "admin",
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
