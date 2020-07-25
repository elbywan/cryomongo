require "bson"
require "../commands"

# The *reIndex* command drops all indexes on a collection and recreates them.
#
# This operation may be expensive for collections that have a large amount of data and/or a large number of indexes.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/reIndex/).
module Mongo::Commands::ReIndex
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey)
    Commands.make({
      reIndex: collection,
      "$db":   database,
    })
  end
end
