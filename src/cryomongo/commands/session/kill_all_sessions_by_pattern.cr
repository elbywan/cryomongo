require "bson"
require "../commands"

# The *killAllSessionsByPattern* command kills all sessions that match any of the specified patterns.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/killAllSessionsByPattern/).
module Mongo::Commands::KillAllSessionsByPattern
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(patterns : Array)
    Commands.make({
      killAllSessionsByPattern: patterns,
      "$db":  "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
