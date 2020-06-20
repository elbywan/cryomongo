require "bson"
require "../commands"

# The *dropConnections* command drops the mongod/mongos instance’s outgoing connections to the specified hosts.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dropConnections/).
module Mongo::Commands::DropConnections
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(host_and_port : Array(String))
    Commands.make({
      dropConnections: 1,
      hostAndPort:     host_and_port,
      "$db":           "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
