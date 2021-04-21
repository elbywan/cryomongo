require "bson"
require "../commands"

# Saves the changes made by the operations in the multi-document transaction and ends the transaction.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/commitTransaction/).
module Mongo::Commands::CommitTransaction
  extend Command
  extend WriteCommand
  extend AlwaysRetryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options = nil)
    Commands.make({
      commitTransaction: 1,
      "$db":             "admin",
    }, options)
  end
end
