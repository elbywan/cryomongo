require "bson"
require "../commands"

# The *cloneCollectionAsCapped* command creates a new capped collection from an existing, non-capped collection within the same database.
# The operation does not affect the original non-capped collection.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/cloneCollectionAsCapped/#dbcmd.cloneCollectionAsCapped).
module Mongo::Commands::CloneCollectionAsCapped
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, to_collection : Collection::CollectionKey, size : Int64, options)
    Commands.make({
      cloneCollectionAsCapped: collection,
      toCollection:            to_collection,
      size:                    size,
      "$db":                   database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
