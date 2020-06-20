require "bson"
require "../commands"

# The *logRotate* command is an administrative command that allows you to rotate the MongoDB logs to prevent a single logfile from consuming too much disk space.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/logRotate/).
module Mongo::Commands::LogRotate
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      logRotate: 1,
      "$db":     "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
