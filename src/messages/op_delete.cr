require "bson"
require "./message_part"
require "./op_code"

# The OP_DELETE message is used to remove one or more documents from a collection.
struct Mongo::Messages::OpDelete < Mongo::Messages::Part

  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::Delete

  @[Flags]
  enum Flags : Int32
    # If set, the database will remove only the first matching document in the collection. Otherwise all matching documents will be removed.
    SingleRemove
  end

  # Integer value of 0. Reserved for future use.
  getter zero : Int32 = 0_i32
  # The full collection name; i.e. namespace.
  getter full_collection_name : String
  # Bit vector to specify flags for the operation.
  getter flags : Flags
  # BSON document that represent the query used to select the documents to be removed.
  # The selector will contain one or more elements, all of which must match for a document to be removed from the collection.
  getter selector : BSON

  def initialize(
    @full_collection_name,
    @flags,
    @selector
  )
  end
end
