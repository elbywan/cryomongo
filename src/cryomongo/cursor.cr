require "bson"
require "./commands"

# A `Cursor` is a pointer to the result set of a query.
#
# This class implements the [`Iterator`](https://crystal-lang.org/api/Iterator.html) module under the hood.
#
# ```
# # Find is one of the methods that return a cursor.
# cursor = collection.find({qty: {"$gt": 20}})
# # Using `to_a` iterates the cursor until the end and stores the elements inside an `Array`.
# elements = cursor.to_a
# # `to_a` is one of the methods inherited from the `Iterator` module.
# ```
class Mongo::Cursor
  include Iterator(BSON)

  @@lock = Mutex.new(:reentrant)
  @database : String
  @collection : Collection::CollectionKey
  @tailable : Bool = false
  @counter : Int32
  @limit : Int32? = nil

  protected property server_description : SDAM::ServerDescription? = nil
  protected property session : Session::ClientSession?

  # :nodoc:
  def initialize(
    @client : Mongo::Client,
    @cursor_id : Int64,
    namespace : String,
    @batch : Array(BSON),
    @batch_size : Int32? = nil,
    @limit : Int32? = nil,
    @await_time_ms : Int64? = nil,
    @tailable : Bool = false,
    @session : Session::ClientSession? = nil
  )
    @counter = @batch.size
    @database, @collection = namespace.split(".", 2)
  end

  # :nodoc:
  def initialize(
    @client : Mongo::Client,
    result : Commands::Common::QueryResult,
    @batch_size : Int32? = nil,
    @limit : Int32? = nil,
    @await_time_ms : Int64? = nil,
    @tailable : Bool = false,
    @session : Session::ClientSession? = nil
  )
    @cursor_id = result.cursor.id
    @batch = result.cursor.first_batch
    @counter = @batch.size
    @database, @collection = result.cursor.ns.split(".", 2)
  end

  # :nodoc:
  def exhausted?
    @cursor_id == 0 || @limit.try { |limit| limit <= @counter }
  end

  def next
    element = @batch.shift?

    if @tailable && !element
      fetch_more
      element = @batch.shift?
    end

    return element if element

    if @cursor_id == 0 || (@tailable && !element)
      Iterator::Stop::INSTANCE
    else
      fetch_more
      self.next
    end
  end

  # Close the cursor and frees underlying resources.
  def close
    @@lock.synchronize {
      unless exhausted?
        if (session = @session) && session.implicit?
          session.end
        end
        self.kill
      end
    }
  rescue e
    # Ignore - client might be dead
  end

  private def kill
    @client.command(
      Commands::KillCursors,
      database: @database,
      collection: @collection,
      cursor_ids: [@cursor_id],
      server_description: @server_description
    )
    @cursor_id = 0_i64
  end

  protected def fetch_more
    return if @cursor_id == 0

    @@lock.synchronize {
      batch_size = @limit.try { |limit| Math.max(limit - @counter, 1) } || @batch_size

      reply = @client.command(
        Commands::GetMore,
        database: @database,
        collection: @collection,
        cursor_id: @cursor_id,
        batch_size: batch_size,
        max_time_ms: @await_time_ms,
        server_description: @server_description,
        session: @session
      ).not_nil!
      @cursor_id = reply.cursor.id
      @batch = reply.cursor.next_batch
      @counter += @batch.size

      if (session = @session) && exhausted? && session.implicit?
        session.end
      end

      reply
    }
  end

  # Will convert the elements to the `T` type while iterating the `Cursor`.
  #
  # Assumes that `T` has a constructor method named `from_bson` that takes a single `BSON` argument.
  #
  # ```
  # # Using .of is shorter than…
  # wrapped_cursor = cursor.of(Type)
  # # …having to .map and initialize.
  # wrapped_cursor = cursor.map { |element| Type.from_bson(element) }
  # ```
  #
  # NOTE: Internally, wraps the cursor inside a `Mongo::Cursor::Wrapper` with type `T`.
  def of(type : T) forall T
    {% begin %}
    Cursor::Wrapper({{T.instance}}).new(self)
    {% end %}
  end

  # Clean up the underlying resource when garbage collected.
  def finalize
    close
  end
end

# A wrapper that will try to convert elements to the underlying `T` type while iterating the `Cursor`.
#
# Assumes that `T` has a constructor method named `from_bson`.
class Mongo::Cursor::Wrapper(T)
  include Iterator(T)

  # :nodoc:
  def initialize(@cursor : Cursor)
  end

  def next
    if (elt = @cursor.next).is_a? Iterator::Stop
      elt
    else
      {% if T == BSON %}
        elt
      {% else %}
        T.from_bson elt
      {% end %}
    end
  end

  delegate :close, to: @cursor
end
