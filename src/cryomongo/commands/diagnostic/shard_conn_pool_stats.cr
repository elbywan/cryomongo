require "bson"
require "../commands"

# Returns information on the pooled and cached connections in the sharded connection pool.
# The command also returns information on the per-thread connection cache in the connection pool.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/shardConnPoolStats/).
module Mongo::Commands::ShardConnPoolStats
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      shardConnPoolStats: 1,
      "$db":              "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
