require "bson"
require "../commands"

# The *listDatabases* command provides a list of all existing databases along with basic statistics about them.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listDatabases/).
module Mongo::Commands::ListDatabases
  extend ReadCommand
  extend Retryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options = nil)
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

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
