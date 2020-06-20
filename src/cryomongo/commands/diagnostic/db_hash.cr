require "bson"
require "../commands"

# Returns the hash values of the collections in a database and an MD5 value for these collections.
# *dbHash* is useful to compare databases across mongod instances, such as across members of replica sets.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dbHash/).
module Mongo::Commands::DbHash
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, options)
    Commands.make({
      dbHash: 1,
      "$db":  database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    bson
  end
end
