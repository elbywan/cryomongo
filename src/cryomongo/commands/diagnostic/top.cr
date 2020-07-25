require "bson"
require "../commands"

# *top* is an administrative command that returns usage statistics for each collection.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/top/).
module Mongo::Commands::Top
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      top:   1,
      "$db": "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
