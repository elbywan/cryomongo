require "bson"
require "../commands"

# Returns information about the indexes on the specified collection.
#
# Specifically, the command returns a document that contains information with which to create a cursor to the index information.
# Index information includes the keys and options used to create the index.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listIndexes/).
module Mongo::Commands::ListIndexes
  extend ReadCommand
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey)
    Commands.make({
      listIndexes: collection,
      "$db":       database,
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::QueryResult.from_bson bson
  end
end
