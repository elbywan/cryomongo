require "./message_part"
require "./op_code"

# The OP_KILL_CURSORS message is used to close an active cursor in the database.
# This is necessary to ensure that database resources are reclaimed at the end of the query.
struct Mongo::Messages::OpKillCursors < Mongo::Messages::Part
  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::KillCursors

  @[Field(ignore: true)]
  getter op_code : OpCode = OpCode::Delete

  # Integer value of 0. Reserved for future use.
  getter zero : Int32 = 0_i32
  # The number of cursor IDs that are in the message.
  getter number_of_cursor_ids : String
  # “Array” of cursor IDs to be closed.
  getter cursor_ids : Array(Int64)

  def initialize(@cursor_ids)
    @number_of_cursor_ids = @cursor_ids.size
  end
end
