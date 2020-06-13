require "bson"
require "../commands"

module Mongo::Commands::Ping
  extend self

  def command
    Commands.make({
      ping: 1,
      "$db": "admin",
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
