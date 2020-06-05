require "bson"
require "../commands"

module Mongo::Commands::Fsync
  extend self

  def command(options)
    Commands.make({
      fsync: 1,
      "$db": "admin"
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
