require "bson"
require "../commands"

module Mongo::Commands::DropConnections
  extend self

  def command(host_and_port : Array(String))
    Commands.make({
      dropConnections: 1,
      hostAndPort: host_and_port,
      "$db": "admin"
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
