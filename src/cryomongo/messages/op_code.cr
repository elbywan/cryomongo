# :nodoc:
module Mongo::Messages
  enum OpCode : Int32
    # Reply to a client request. responseTo is set.
    Reply = 1
    # Update document.
    Update = 2001
    # Insert new document.
    Insert = 2002
    # Formerly used for OP_GET_BY_OID.
    Reserved = 2003
    # Query a collection.
    Query = 2004
    # Get more data from a query. See Cursors.
    GetMore = 2005
    # Delete documents.
    Delete = 2006
    # Notify database that the client has finished with the cursor.
    KillCursors = 2007
    # Cluster internal protocol representing a command request.
    Command = 2010
    # Cluster internal protocol representing a reply to an OP_COMMAND.
    CommandReply = 2011
    # Send a message using the format introduced in MongoDB 3.6.
    Msg = 2013
  end
end
