require "bson"
require "../commands"

# The *ping* command is a no-op used to test whether a server is responding to commands.
# This command will return immediately even if the server is write-locked.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/ping/).
module Mongo::Commands::Ping
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      ping:  1,
      "$db": "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
