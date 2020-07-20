require "bson"
require "../commands"

# The *explain* command provides information on the execution of the following commands: *aggregate*, *count*, *distinct*, *find*, *findAndModify*, *delete*, and *update*.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/explain/).
module Mongo::Commands::Explain
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, explain, options)
    Commands.make({
      explain: BSON.new(explain),
      "$db":   database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
