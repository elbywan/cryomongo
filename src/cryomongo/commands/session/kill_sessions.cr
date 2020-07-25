require "bson"
require "../commands"

# The *killSessions* command kills the specified sessions.
#
# If access control is enabled, the command only kills the sessions owned by the user.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/killSessions/).
module Mongo::Commands::KillSessions
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(ids : Array)
    Commands.make({
      killSessions: ids,
      "$db":  "admin",
    })
  end
end
