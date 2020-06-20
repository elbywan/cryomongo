require "bson"
require "../commands"

# The *dropIndexes* command drops one or more indexes (except the index on the _id field) from the specified collection.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dropIndexes/).
module Mongo::Commands::DropIndexes
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, index, options)
    Commands.make({
      dropIndexes: collection,
      index:       index,
      "$db":       database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
