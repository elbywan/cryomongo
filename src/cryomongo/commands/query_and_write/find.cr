require "bson"
require "../commands"

# Executes a *query* and returns the first batch of results and the cursor id, from which the client can construct a cursor.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/find/).
module Mongo::Commands::Find
  extend ReadCommand
  extend MayUseSecondary
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, filter, options = nil)
    Commands.make({
      find:   collection,
      filter: BSON.new(filter),
      "$db":  database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::QueryResult.from_bson bson
  end
end
