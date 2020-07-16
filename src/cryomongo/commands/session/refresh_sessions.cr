require "bson"
require "../commands"

# The *refreshSessions* command updates the last use time for the specified sessions, thereby extending the active state of the sessions.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/refreshSessions/).
module Mongo::Commands::RefreshSessions
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(ids : Array)
    Commands.make({
      refreshSessions: ids,
      "$db":  "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
