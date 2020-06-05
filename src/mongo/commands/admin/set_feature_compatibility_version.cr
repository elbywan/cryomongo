require "bson"
require "../commands"

module Mongo::Commands::SetFeatureCompatibilityVersion
  extend self

  def command(version : String)
    Commands.make({
      setFeatureCompatibilityVersion: version,
      "$db": "admin"
    })
  end

  def result(bson)
    Common::BaseResult.from_bson bson
  end
end
