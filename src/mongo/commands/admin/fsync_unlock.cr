require "bson"
require "../commands"

module Mongo::Commands::FsyncUnlock
  extend self

  def command
    Commands.make({
      fsyncUnlock: 1,
      "$db": "admin"
    })
  end

  Common.result(Result) {
    property info : String
    property lock_count : Int64
  }

  def result(bson)
    Result.from_bson bson
  end
end
