require "bson"
require "../commands"

# Finds the distinct values for a specified field across a single collection, and returns a document that contains an array of the distinct values.
# The return document also contains an embedded document with query statistics and the query plan.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/distinct/).
module Mongo::Commands::Distinct
  extend ReadCommand
  extend MayUseSecondary
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, key : String, options = nil)
    Commands.make({
      distinct: collection,
      key:      key,
      "$db":    database,
    }, options)
  end

  Common.result(Result) {
    property values : BSON
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
