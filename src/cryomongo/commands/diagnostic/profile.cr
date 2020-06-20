require "bson"
require "../commands"

module Mongo::Commands::Profile
  extend self

  def command(level : Int32, options)
    Commands.make({
      profile: level,
      "$db":   "admin",
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
