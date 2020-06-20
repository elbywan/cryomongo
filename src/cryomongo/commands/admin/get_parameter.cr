require "bson"
require "../commands"

# *getParameter* is an administrative command for retrieving the values of parameters.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/getParameter/).
module Mongo::Commands::GetParameter
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
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

  # Transforms the server result.
  def result(bson : BSON)
    Result.from_bson bson
  end
end
