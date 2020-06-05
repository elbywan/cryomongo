require "bson"
require "../commands"

module Mongo::Commands::LogRotate
  extend self

  def command
    Commands.make({
      logRotate: 1,
      "$db": "admin"
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
