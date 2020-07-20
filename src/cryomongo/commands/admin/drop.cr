require "bson"
require "../commands"

# The *drop* command removes an entire collection from a database.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/drop/).
module Mongo::Commands::Drop
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, name : Collection::CollectionKey, options = nil)
    Commands.make({
      drop:  name,
      "$db": database,
    }, options)
  end
end
