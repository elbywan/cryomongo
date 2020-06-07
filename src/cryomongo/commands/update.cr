require "bson"
require "./commands"

module Mongo::Commands::Update
  extend self

  def command(database : String, collection : Collection::CollectionKey, updates : Array, options)
    Commands.make({
      update: collection,
      "$db":  database,
    }, sequences: {
      updates: updates.map { |elt| BSON.new(elt) },
    }, options: options)
  end

  def result(bson)
    Common::UpdateResult.from_bson bson
  end
end
