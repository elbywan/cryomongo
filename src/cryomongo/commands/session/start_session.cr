require "bson"
require "../commands"

# The *startSession* command starts a new logical session for a sequence of operations.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/startSession/).
module Mongo::Commands::StartSession
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(cluster_time = nil)
    Commands.make({
      startSession: 1,
      "$db":  "admin",
    }, options: {
      "$clusterTime": cluster_time
    })
  end

  Common.result(Result) {
    property id : ID?
    property timeout_minutes : Int32?

    Common.result(ID, root: false) {
      id : String
    }
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
