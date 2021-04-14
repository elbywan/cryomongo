require "bson"
require "../commands"

# Terminates the multi-document transaction and rolls back any data changes made by the operations within the transaction.
# That is, the transaction ends without saving any of the changes made by the operations in the transaction.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/abortTransaction/).
module Mongo::Commands::AbortTransaction
  extend Command
  extend WriteCommand
  extend AlwaysRetryable
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options = nil)
    Commands.make({
      abortTransaction: 1,
      "$db":            "admin",
    }, options)
  end
end
