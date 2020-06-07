require "bson"
require "../commands"

module Mongo::Commands::GetParameter
  extend self

  def command(parameter : String?)
    bson, _ = Commands.make({
      getParameter: parameter.nil? ? "*" : 1,
      "$db":        "admin",
    })
    bson[parameter] = 1 unless parameter.nil?
    {bson, nil}
  end

  Common.result(Result) {
    property info : String
    property lock_count : Int64
  }

  def result(bson)
    Result.from_bson bson
  end
end
