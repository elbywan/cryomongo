require "bson"
require "../commands"

# The *getCmdLineOpts* command returns a document containing command line options used to start the given mongod or mongos.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/getCmdLineOpts/).
module Mongo::Commands::GetCmdLineOpts
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command
    Commands.make({
      getCmdLineOpts: 1,
      "$db":          "admin",
    })
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
