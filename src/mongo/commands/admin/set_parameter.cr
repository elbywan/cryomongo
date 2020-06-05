require "bson"
require "../commands"

module Mongo::Commands::SetParameter
  extend self

  def command(parameter : String, value)
    bson, _ = Commands.make({
      setParameter: 1,
      "$db": "admin"
    })
    bson[parameter] = value
    { bson, nil }
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
