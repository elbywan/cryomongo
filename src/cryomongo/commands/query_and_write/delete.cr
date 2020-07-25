require "bson"
require "../commands"

# The *delete* command removes documents from a collection.
# A single delete command can contain multiple delete specifications.
# The command cannot operate on capped collections.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/delete/).
module Mongo::Commands::Delete
  extend WriteCommand
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, deletes : Array, options = nil)
    Commands.make({
      delete: collection,
      "$db":  database,
    }, sequences: {
      deletes: deletes.map { |elt| BSON.new(elt) },
    }, options: options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::DeleteResult.from_bson bson
  end

  def retryable?(**args)
    if deletes = args["deletes"]?
      deletes.size == 1 && deletes[0]["limit"]? == 1
    end
  end
end
