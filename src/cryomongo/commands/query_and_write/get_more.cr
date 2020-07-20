require "bson"
require "../../collection"
require "../commands"

# Use in conjunction with commands that return a cursor, e.g. *find* and *aggregate*, to return subsequent batches of documents currently pointed to by the cursor.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/getMore/).
module Mongo::Commands::GetMore
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, cursor_id : Int64, **options)
    Commands.make({
      getMore:    cursor_id,
      collection: collection,
      "$db":      database,
    }, options)
  end

  Common.result(Result) {
    property cursor : Cursor

    Common.result(Cursor, root: false) {
      property id : Int64
      property ns : String
      property next_batch : Array(BSON)
      property post_batch_resume_token : BSON?
    }
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
