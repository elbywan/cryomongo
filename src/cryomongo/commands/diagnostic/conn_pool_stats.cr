require "bson"
require "../commands"

# The command *connPoolStats* returns information regarding the open outgoing connections from the current database
# instance to other members of the sharded cluster or replica set.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/connPoolStats/).
module Mongo::Commands::ConnPoolStats
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      connPoolStats: 1,
      "$db":         "admin",
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
