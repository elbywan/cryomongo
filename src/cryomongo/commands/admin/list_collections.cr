require "bson"
require "../commands"

# Retrieve information, i.e. the name and options, about the collections and views in a database.
#
# Specifically, the command returns a document that contains information with which to create a cursor to the collection information.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listCollections/).
module Mongo::Commands::ListCollections
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, options)
    Commands.make({
      listCollections: 1,
      "$db":           database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::QueryResult.from_bson bson
  end
end
