require "./message_part"
require "./op_code"

# The OP_GET_MORE message is used to query the database for documents in a collection.
struct Mongo::Messages::OpGetMore < Mongo::Messages::Part

  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::GetMore

  # Integer value of 0. Reserved for future use.
  getter zero : Int32 = 0_i32
  # The full collection name; i.e. namespace.
  getter full_collection_name : String
  # Limits the number of documents in the first OP_REPLY message to the query.
  # However, the database will still establish a cursor and return the cursorID to the client if there are more results than numberToReturn.
  # If the client driver offers ‘limit’ functionality (like the SQL LIMIT keyword),
  # then it is up to the client driver to ensure that no more than the specified number of document are returned to the calling application.
  # If numberToReturn is 0, the db will used the default return size.
  getter number_to_return : Int32
  # Cursor identifier that came in the OP_REPLY. This must be the value that came from the database.
  getter cursor_id : Int64

  def initialize(
    @full_collection_name,
    @number_to_return,
    @cursor_id
  )
  end
end
