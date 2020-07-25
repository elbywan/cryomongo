require "bson"
require "../commands"

# The *killAllSessions* command kills all sessions for the specified users.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/killAllSessions/).
module Mongo::Commands::KillAllSessions
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(users : Array)
    Commands.make({
      killAllSessions: users,
      "$db":  "admin",
    })
  end
end
