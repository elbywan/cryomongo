require "bson"
require "../commands"

# Returns a document with information about the underlying system that the mongod or mongos runs on.
# Some of the returned fields are only included on some platforms.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/hostInfo/).
module Mongo::Commands::HostInfo
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      hostInfo: 1,
      "$db":    "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
