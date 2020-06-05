require "bson"
require "../commands"

module Mongo::Commands::Shutdown
  extend self

  def command(options)
    Commands.make({
      shutdown: 1,
      "$db": "admin"
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
