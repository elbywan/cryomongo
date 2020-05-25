require "bson"
require "../commands"

module Mongo::Commands::KillCursors
  extend self

  def command(database : String, collection : Collection::CollectionKey, cursor_ids : Array(Int64))
    Commands.make({
      killCursors: collection,
      cursors: cursor_ids,
      "$db": database
    })
  end

  Common.result(Result) {
    property cursors_killed : Array(Int64)
    property cursors_not_found : Array(Int64)
    property cursors_alive : Array(Int64)
    property cursors_unknown : Array(Int64)
  }

  def result(bson)
    Result.from_bson bson
  end
end
