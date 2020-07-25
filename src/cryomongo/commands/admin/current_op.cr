require "bson"
require "../commands"

# Returns a document that contains information on in-progress operations for the mongod instance.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/currentOp/).
module Mongo::Commands::CurrentOp
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options = nil)
    Commands.make({
      currentOp: 1,
      "$db":     "admin",
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
