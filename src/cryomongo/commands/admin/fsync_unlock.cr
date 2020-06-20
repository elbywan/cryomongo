require "bson"
require "../commands"

# Reduces the lock taken by *fsync* (with the lock option) on a mongod instance by 1.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/fsyncUnlock/).
module Mongo::Commands::FsyncUnlock
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      fsyncUnlock: 1,
      "$db":       "admin",
    })
  end

  Common.result(Result) {
    property info : String
    property lock_count : Int64
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
