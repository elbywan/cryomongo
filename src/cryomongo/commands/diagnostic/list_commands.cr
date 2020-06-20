require "bson"
require "../commands"

# The *listCommands* command generates a list of all database commands implemented for the current mongod or mongos instance.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listCommands/).
module Mongo::Commands::ListCommands
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      listCommands: 1,
      "$db":        "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
