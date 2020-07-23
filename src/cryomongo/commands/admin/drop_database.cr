require "bson"
require "../commands"

# The *dropDatabase* command drops the current database, deleting the associated data files.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dropDatabase/).
module Mongo::Commands::DropDatabase
  extend WriteCommand
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, options = nil)
    Commands.make({
      dropDatabase: 1,
      "$db":        database,
    }, options)
  end
end
