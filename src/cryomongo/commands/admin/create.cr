require "bson"
require "../commands"

# Explicitly creates a collection or view.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/create/).
module Mongo::Commands::Create
  extend WriteCommand
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, name : Collection::CollectionKey, options = nil)
    Commands.make({
      create: name,
      "$db":  database,
    }, options)
  end
end
