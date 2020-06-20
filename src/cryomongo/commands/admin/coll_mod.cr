require "bson"
require "../commands"

# *collMod* makes it possible to add options to a collection or to modify view definitions.
#
# NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/collMod/).
module Mongo::Commands::CollMod
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(database : String, collection : Collection::CollectionKey, options)
    Commands.make({
      collMod: collection,
      "$db":   database,
    }, options)
  end

  # Transforms the server result.
  def result(bson : BSON)
    Common::BaseResult.from_bson bson
  end
end
