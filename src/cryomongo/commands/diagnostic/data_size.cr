require "bson"
require "../commands"

# The *dataSize* command returns the data size for a set of data within a certain range.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dataSize/).
module Mongo::Commands::DataSize
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      dataSize: collection,
      "$db":    database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
