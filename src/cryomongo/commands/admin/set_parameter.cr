require "bson"
require "../commands"

# *setParameter* is an administrative command for modifying options normally set on the command line.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/setParameter/).
module Mongo::Commands::SetParameter
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(parameter : String, value)
    bson, _ = Commands.make({
      setParameter: 1,
      "$db":        "admin",
    })
    bson[parameter] = value
    {bson, nil}
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
