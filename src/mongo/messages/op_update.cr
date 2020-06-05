require "bson"
require "./message_part"
require "./op_code"

# The OP_UPDATE message is used to update a document in a collection.
struct Mongo::Messages::OpUpdate < Mongo::Messages::Part

  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::Update

  @[Flags]
  enum Flags : Int32
    # If set, the database will insert the supplied object into the collection if no matching document is found.
    Upsert
    # If set, the database will update all matching objects in the collection. Otherwise only updates first matching document.
    MultiUpdate
  end

  # 0 - reserved for future use
  getter zero : Int32 = 0_i32
  # The full collection name; i.e. namespace.
  getter full_collection_name : String
  # Bit vector to specify flags for the operation.
  getter flags : Flags
  # the query to select the document
  getter selector : BSON
  # specification of the update to perform
  getter update : BSON

  def initialize(
    @full_collection_name,
    @flags : Flags,
    @selector,
    @update)
  end
end
