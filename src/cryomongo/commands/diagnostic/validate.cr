require "bson"
require "../commands"

# The *validate* command checks a collection’s data and indexes for correctness and returns the results.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/validate/).
module Mongo::Commands::Validate
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      validate: collection,
      "$db":    database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
