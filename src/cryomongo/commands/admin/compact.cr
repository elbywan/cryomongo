require "bson"
require "../commands"

# Rewrites and defragments all data and indexes in a collection.
# On WiredTiger databases, this command will release unneeded disk space to the operating system.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/compact/).
module Mongo::Commands::Compact
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, options = nil)
    Commands.make({
      compact: collection,
      "$db":   database,
    }, options)
  end
end
