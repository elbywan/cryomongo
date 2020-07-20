require "bson"
require "../commands"

# Terminates an operation as specified by the operation ID.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/killOp/).
module Mongo::Commands::KillOp
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(op : Int32)
    Commands.make({
      killOp: 1,
      op:     op,
      "$db":  "admin",
    })
  end
end
