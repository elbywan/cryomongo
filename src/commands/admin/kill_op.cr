require "bson"
require "../commands"

module Mongo::Commands::KillOp
  extend self

  def command(op : Int32)
    Commands.make({
      killOp: 1,
      op: op,
      "$db": "admin"
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
