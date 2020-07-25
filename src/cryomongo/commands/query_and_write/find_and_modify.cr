require "bson"
require "../commands"

# The *findAndModify* command modifies and returns a single document.
# By default, the returned document does not include the modifications made on the update.
# To return the document with the modifications made on the update, use the new option.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/findAndModify/).
module Mongo::Commands::FindAndModify
  extend WriteCommand
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, filter, options = nil)
    Commands.make({
      findAndModify: collection,
      query:         BSON.new(filter),
      "$db":         database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
