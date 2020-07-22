require "bson"
require "../commands"

# The *dbStats* command returns storage statistics for a given database.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dbStats/).
module Mongo::Commands::DbStats
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, options = nil)
    Commands.make({
      dbStats: 1,
      "$db":   database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
