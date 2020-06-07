require "bson"
require "../commands"

module Mongo::Commands::ListDatabases
  extend self

  def command(options)
    Commands.make({
      listDatabases: 1,
      "$db":         "admin",
    }, options)
  end

  Common.result(Result) {
    property databases : Array(Database)?
    property total_size : Float64?

    Common.result(Database, root: false) {
      property name : String
      property size_on_disk : Float64?
      property empty : Bool?
      property shards : BSON?
    }
  }

  def result(bson)
    Result.from_bson bson
  end
end
