require "bson"
require "../commands"

# The *endSessions* command expires the specified sessions. The command overrides the timeout period that sessions wait before expiring.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/endSessions/).
module Mongo::Commands::EndSessions
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(ids : Array)
    Commands.make({
      endSessions: ids,
      "$db":  "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
