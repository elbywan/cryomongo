require "bson"
require "../commands"

# The *collStats* command returns a variety of storage statistics for a given collection.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/collStats/).
module Mongo::Commands::CollStats
  extend Command
  extend MayUseSecondary
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, options = nil)
    Commands.make({
      collStats: collection,
      "$db":     database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
