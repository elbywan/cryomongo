require "bson"
require "../commands"

# The convertToCapped command converts an existing, non-capped collection to a capped collection within the same database.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/convertToCapped/).
module Mongo::Commands::ConvertToCapped
  extend WriteCommand
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, size : Int64, options = nil)
    Commands.make({
      convertToCapped: collection,
      size:            size,
      "$db":           database,
    }, options)
  end
end
