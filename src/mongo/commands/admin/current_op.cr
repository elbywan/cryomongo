require "bson"
require "../commands"

module Mongo::Commands::CurrentOp
  extend self

  def command(options)
    Commands.make({
      currentOp: 1,
      "$db": "admin"
    }, options)
  end

  def result(bson)
    bson
  end
end
