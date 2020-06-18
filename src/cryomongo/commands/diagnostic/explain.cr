require "bson"
require "../commands"

module Mongo::Commands::Explain
  extend self

  def command(database : String, explain, options)
    Commands.make({
      explain: BSON.new(explain),
      "$db":    database,
    }, options)
  end

  def result(bson)
    bson
  end
end
