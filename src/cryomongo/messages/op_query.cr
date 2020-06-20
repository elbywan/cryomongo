require "bson"
require "./message_part"
require "./op_code"

# The OP_QUERY message is used to query the database for documents in a collection.
struct Mongo::Messages::OpQuery < Mongo::Messages::Part
  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::Query

  @[Flags]
  enum Flags : Int32
    # Tailable means cursor is not closed when the last data is retrieved.
    # Rather, the cursor marks the final object’s position.
    # You can resume using the cursor later, from where it was located, if more data were received.
    # Like any “latent cursor”, the cursor may become invalid at some point (CursorNotFound)
    # – for example if the final object it references were deleted.
    TailableCursor
    # Allow query of replica slave. Normally these return an error except for namespace “local”.
    SlaveOk
    # Internal replication use only - driver should not set.
    OplogReplay
    # The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.
    # Set this option to prevent that.
    NoCursorTimeout
    # Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data.
    # After a timeout period, we do return as normal.
    AwaitData
    # Stream the data down full blast in multiple “more” packages, on the assumption that the client will fully read all data queried.
    # Faster when you are pulling a lot of data and know you want to pull it all down.
    # NOTE: the client is not allowed to not read all the data unless it closes the connection.
    Exhaust
    # Get partial results from a mongos if some shards are down. (instead of throwing an error)
    Partial
  end

  # Bit vector to specify flags for the operation.
  getter flags : Flags
  # The full collection name; i.e. namespace.
  getter full_collection_name : String
  # Sets the number of documents to omit - starting from the first document in the resulting dataset - when returning the result of the query.
  getter number_to_skip : Int32
  # Limits the number of documents in the first OP_REPLY message to the query.
  # However, the database will still establish a cursor and return the cursorID to the client if there are more results than numberToReturn.
  # If the client driver offers ‘limit’ functionality (like the SQL LIMIT keyword),
  # then it is up to the client driver to ensure that no more than the specified number of document are returned to the calling application.
  # If numberToReturn is 0, the db will use the default return size. If the number is negative,
  # then the database will return that number and close the cursor. No further results for that query can be fetched.
  # If numberToReturn is 1 the server will treat it as -1 (closing the cursor automatically).
  getter number_to_return : Int32
  # BSON document that represents the query.
  # The query will contain one or more elements, all of which must match for a document to be included in the result set.
  # Possible elements include $query, $orderby, $hint, and $explain.
  getter query : BSON
  # Optional. BSON document that limits the fields in the returned documents.
  # The returnFieldsSelector contains one or more elements, each of which is the name of a field that should be returned, and and the integer value 1.
  getter return_fields_selector : BSON?

  def initialize(
    @full_collection_name,
    @flags : Flags,
    @number_to_skip,
    @number_to_return,
    @query,
    @return_fields_selector = nil
  )
  end
end
