require "bson"
require "../commands"

# Kills the specified cursor or cursors for a collection.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/killCursors/).
module Mongo::Commands::KillCursors
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, cursor_ids : Array(Int64))
    Commands.make({
      killCursors: collection,
      cursors:     cursor_ids,
      "$db":       database,
    })
  end

  Common.result(Result) {
    property cursors_killed : Array(Int64)
    property cursors_not_found : Array(Int64)
    property cursors_alive : Array(Int64)
    property cursors_unknown : Array(Int64)
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
