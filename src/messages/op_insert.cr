require "bson"
require "./message_part"
require "./op_code"

# The OP_INSERT message is used to insert one or more documents into a collection.
struct Mongo::Messages::OpInsert < Mongo::Messages::Part

  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::Insert

  @[Flags]
  enum Flags : Int32
    #  If set, the database will not stop processing a bulk insert if one fails (eg due to duplicate IDs).
    #  This makes bulk insert behave similarly to a series of single inserts, except lastError will be set if any insert fails,
    #  not just the last one. If multiple errors occur, only the most recent will be reported by getLastError.
    ContinueOnError
  end

  # Bit vector to specify flags for the operation.
  getter flags : Flags
  # The full collection name; i.e. namespace.
  getter full_collection_name : String
  # one or more documents to insert into the collection
  getter documents : Array(BSON)

  def initialize(@full_collection_name, @flags : Flags, @documents)
  end
end
