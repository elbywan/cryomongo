require "bson"
require "../commands"

# The *buildInfo* command is an administrative command which returns a build summary for the current mongod.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/buildInfo/).
module Mongo::Commands::BuildInfo
  extend Command
  extend self

  # The command name.
  def name
    "buildInfo"
  end

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      buildInfo: 1,
      "$db":     "admin",
    })
  end

  Common.result(Result) {
    property version : String?
    property git_version : String?
    property sys_info : String?
    property loader_flags : String?
    property compiler_flags : String?
    property allocator : String?
    property version_array : Array(Float64)?
    property openssl : BSON?
    property javascript_engine : String?
    property bits : Float64?
    property debug : Bool?
    property max_bson_object_size : Float64?
  }

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
