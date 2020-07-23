require "bson"
require "../commands"

# The *insert* command inserts one or more documents and returns a document containing the status of all inserts.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/insert/).
module Mongo::Commands::Insert
  extend WriteCommand
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, documents : Array, options)
    Commands.make({
      insert: collection,
      "$db":  database,
    }, sequences: {
      documents: documents.map { |elt| BSON.new(elt) },
    }, options: options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::InsertResult.from_bson bson
  end
end
