require "bson"
require "../commands"

# Enables or disables the features that persist data incompatible with earlier versions of MongoDB.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/setFeatureCompatibilityVersion/).
module Mongo::Commands::SetFeatureCompatibilityVersion
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(version : String)
    Commands.make({
      setFeatureCompatibilityVersion: version,
      "$db":                          "admin",
    })
  end
end
