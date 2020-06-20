require "bson"
require "../commands"

# For a mongod instance, the command enables, disables, or configures the Database Profiler.
# For mongos instance, the command sets the slowms and sampleRate configuration settings, which configure how operations get written to the diagnostic log.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/profile/).
module Mongo::Commands::Profile
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(level : Int32, options)
    Commands.make({
      profile: level,
      "$db":   "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
