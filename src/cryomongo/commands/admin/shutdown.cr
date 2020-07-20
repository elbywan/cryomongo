require "bson"
require "../commands"

# The *shutdown* command cleans up all database resources and then terminates the process.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/shutdown/).
module Mongo::Commands::Shutdown
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options)
    Commands.make({
      shutdown: 1,
      "$db":    "admin",
    }, options)
  end
end
