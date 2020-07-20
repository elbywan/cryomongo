require "bson"
require "../commands"

# The *serverStatus* command returns a document that provides an overview of the database’s state.
# Monitoring applications can run this command at a regular interval to collect statistics about the instance.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/serverStatus/).
module Mongo::Commands::ServerStatus
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options)
    Commands.make({
      serverStatus: 1,
      "$db":        "admin",
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
