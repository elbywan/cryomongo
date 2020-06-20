require "bson"
require "../commands"

module Mongo::Commands::ShardConnPoolStats
  extend self

  def command
    Commands.make({
      shardConnPoolStats: 1,
      "$db":              "admin",
    })
  end

  def result(bson)
    bson
  end
end
