require "bson"
require "../commands"

# Changes the name of an existing collection.
#
# Specify collection names to renameCollection in the form of a complete namespace (<database>.<collection>).
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/renameCollection/).
module Mongo::Commands::RenameCollection
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, to : String, options)
    Commands.make({
      renameCollection: "#{database}.#{collection}",
      to:               to,
      "$db":            "admin",
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
