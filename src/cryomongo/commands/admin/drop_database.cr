require "bson"
require "../commands"

module Mongo::Commands::DropDatabase
  extend self

  def command(database : String, options = nil)
    Commands.make({
      dropDatabase: 1,
      "$db": database,
    }, options)
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
