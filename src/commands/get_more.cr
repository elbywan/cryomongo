require "bson"
require "../collection"
require "./commands"

module Mongo::Commands::GetMore
  extend self

  def command(database : String, collection : Collection::CollectionKey, cursor_id : Int64, **options)
    Commands.make({
      getMore: cursor_id,
      collection: collection,
      "$db": database
    }, options)
  end

  Common.result(Result) {
    property cursor : Cursor

    Common.result(Cursor, root: false) {
      property id : Int64
      property ns : String
      property next_batch : Array(BSON)
    }
  }

  def result(bson)
    Result.from_bson bson
  end
end
