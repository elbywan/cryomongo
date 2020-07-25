require "bson"
require "../commands"

# Counts the number of documents in a collection or a view.
# Returns a document that contains this count and as well as the command status.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/count/).
module Mongo::Commands::Count
  extend ReadCommand
  extend MayUseSecondary
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, options = nil)
    Commands.make({
      count: collection,
      "$db": database,
    }, options) { |key|
      key == "query" && key.nil?
    }
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
