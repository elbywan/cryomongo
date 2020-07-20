require "bson"
require "../commands"

# Forces the mongod process to flush all pending writes from the storage layer to disk and locks the entire mongod instance
# to prevent additional writes until the user releases the lock with a corresponding *fsyncUnlock*.
#
# Optionally, you can use *fsync* to lock the mongod instance and block write operations for the purpose of capturing backups.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/fsync/).
module Mongo::Commands::Fsync
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(options)
    Commands.make({
      fsync: 1,
      "$db": "admin",
    }, options)
  end
end
