require "./client"
require "./collection"
require "./concerns"
require "./read_preference"
require "./gridfs"

# A `Database` provides access to a MongoDB database.
#
# ```
# database = client["database_name"]
# ```
class Mongo::Database
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  # The underlying MongoDB client.
  getter client : Mongo::Client
  # The database name.
  getter name : String

  # :nodoc:
  def initialize(@client, @name)
  end

  # Execute a command on the server targeting the database.
  #
  # Will automatically set the *database* arguments.
  #
  # See: `Mongo::Client.command`
  def command(
    operation,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil,
    **args
  )
    @client.command(
      operation,
      **args,
      database: @name,
      write_concern: write_concern || @write_concern,
      read_concern: read_concern || @read_concern,
      read_preference: read_preference || @read_preference,
      session: session
    )
  end

  # Get a newly allocated `Mongo::Collection` for the collection named *name*.
  def collection(collection : Collection::CollectionKey) : Mongo::Collection
    Collection.new(self, collection)
  end

  # :ditto:
  def [](collection : Collection::CollectionKey) : Mongo::Collection
    self.collection(collection)
  end

  # Runs an aggregation framework pipeline on the database for pipeline stages
  # that do not require an underlying collection, such as $currentOp and $listLocalSessions.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/aggregate/).
  def aggregate(
    pipeline : Array,
    *,
    allow_disk_use : Bool? = nil,
    batch_size : Int32? = nil,
    max_time_ms : Int64? = nil,
    bypass_document_validation : Bool? = nil,
    read_concern : ReadConcern? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    comment : String? = nil,
    write_concern : WriteConcern? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::Cursor? forall H
    maybe_result = self.command(Commands::Aggregate, collection: 1, pipeline: pipeline, session: session, options: {
      allow_disk_use:             allow_disk_use,
      cursor:                     batch_size.try { {batch_size: batch_size} },
      bypass_document_validation: bypass_document_validation,
      read_concern:               read_concern,
      collation:                  collation,
      hint:                       hint.is_a?(String) ? hint : BSON.new(hint),
      comment:                    comment,
      write_concern:              write_concern
    })
    maybe_result.try { |result| Cursor.new(@client, result, batch_size: batch_size, session: session) }
  end

  # Retrieve information, i.e. the name and options, about the collections and views in a database.
  #
  # Specifically, the command returns a document that contains information with which to create a cursor to the collection information.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listCollections/).
  def list_collections(
    *,
    filter = nil,
    name_only : Bool? = nil,
    authorized_collections : Bool? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::Cursor
    result = self.command(Commands::ListCollections, session: session, options: {
      filter:                 filter,
      name_only:              name_only,
      authorized_collections: authorized_collections
    }).not_nil!
    Cursor.new(@client, result, session: session)
  end

  # Returns a `Mongo::GridFS` instance configured with the arguments provided.
  #
  # NOTE: [for more details about GridFS, please check the official MongoDB manual](https://docs.mongodb.com/manual/core/gridfs/).
  def grid_fs(
    bucket_name : String = "fs",
    *,
    chunk_size_bytes : Int32 = 255 * 1024,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil
  ) : GridFS::Bucket
    GridFS::Bucket.new(
      self,
      bucket_name: bucket_name,
      chunk_size_bytes: chunk_size_bytes,
      write_concern: write_concern,
      read_concern: read_concern,
      read_preference: read_preference
    )
  end

  # Returns a `ChangeStream::Cursor` watching all the database collection.
  #
  # NOTE: Excludes system collections.
  #
  # ```
  # client = Mongo::Client.new
  # database = client["db"]
  #
  # spawn {
  #   cursor = database.watch(
  #     [
  #       {"$match": {"operationType": "insert"}},
  #     ],
  #     max_await_time_ms: 10000
  #   )
  #   # cursor.of(BSON) converts to the Mongo::ChangeStream::Document(BSON) type.
  #   cursor.of(BSON).each { |doc|
  #     puts doc.to_bson.to_json
  #   }
  # }
  #
  # 100.times do |i|
  #   database["collection"].insert_one({count: i})
  # end
  #
  # sleep
  # ```
  #
  # NOTE: [for more details, please check the official manual](https://docs.mongodb.com/manual/changeStreams/index.html).
  def watch(
    pipeline : Array = [] of BSON,
    *,
    full_document : String? = nil,
    resume_after : BSON? = nil,
    max_await_time_ms : Int64? = nil,
    batch_size : Int32? = nil,
    collation : Collation? = nil,
    start_at_operation_time : Time? = nil,
    start_after  : BSON? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::ChangeStream::Cursor
    ChangeStream::Cursor.new(
      client: @client,
      database: name,
      collection: 1,
      pipeline: pipeline.map { |elt| BSON.new(elt) },
      full_document: full_document,
      resume_after: resume_after,
      start_after: start_after,
      start_at_operation_time: start_at_operation_time,
      read_concern: read_concern,
      read_preference: read_preference,
      max_time_ms: max_await_time_ms,
      batch_size: batch_size,
      collation: collation,
      session: session
    )
  end

  # Returns a variety of storage statistics for the database.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/dbStats/).
  def stats(*, scale : Int32? = nil) : BSON?
    self.command(Commands::DbStats, options: {scale: scale})
  end
end
