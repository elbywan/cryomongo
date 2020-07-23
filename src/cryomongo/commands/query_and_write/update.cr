require "bson"
require "../commands"

# The *update* command modifies documents in a collection.
# A single update command can contain multiple update statements.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/update/).
module Mongo::Commands::Update
  extend WriteCommand
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, updates : Array, options)
    Commands.make({
      update: collection,
      "$db":  database,
    }, sequences: {
      updates: updates.map { |elt| BSON.new(elt) },
    }, options: options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::UpdateResult.from_bson bson
  end

  def retryable?(**args)
    args.dig?(:options, :multi).try &.== false
  end
end
