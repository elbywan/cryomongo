require "bson"
require "../commands"

module Mongo::Commands::BuildInfo
  extend self

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

  def result(bson)
    Result.from_bson bson
  end
end
