require "./client"
require "./collection"
require "./concerns"
require "./read_preference"
require "./gridfs"

class Mongo::Database
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  getter client : Mongo::Client
  getter name : String

  def initialize(@client, @name)
  end

  def command(operation, write_concern : WriteConcern? = nil, read_concern : ReadConcern? = nil, read_preference : ReadPreference? = nil, **args)
    @client.command(operation, **args, database: @name, write_concern: write_concern || @write_concern, read_concern: read_concern || @read_concern, read_preference: read_preference || @read_preference)
  end

  def collection(collection : Collection::CollectionKey) : Mongo::Collection
    Collection.new(self, collection)
  end

  def [](collection : Collection::CollectionKey) : Mongo::Collection
    self.collection(collection)
  end

  # Runs an aggregation framework pipeline on the database for pipeline stages
  # that do not require an underlying collection, such as $currentOp and $listLocalSessions.
  #
  # Note: result iteration should be backed by a cursor. Depending on the implementation,
  # the cursor may back the returned Iterable instance or an iterator that it produces.
  #
  # See: https://docs.mongodb.com/manual/reference/command/aggregate/#dbcmd.aggregate
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
    write_concern : WriteConcern? = nil
  ) : Mongo::Cursor? forall H
    maybe_result = self.command(Commands::Aggregate, collection: 1, pipeline: pipeline, options: {
      allow_disk_use:             allow_disk_use,
      cursor:                     batch_size.try { {batch_size: batch_size} },
      bypass_document_validation: bypass_document_validation,
      read_concern:               read_concern,
      collation:                  collation,
      hint:                       hint.is_a?(String) ? hint : BSON.new(hint),
      comment:                    comment,
      write_concern:              write_concern,
    })
    maybe_result.try { |result| Cursor.new(@client, result) }
  end

  def list_collections(
    *,
    filter = nil,
    name_only : Bool? = nil,
    authorized_collections : Bool? = nil
  ) : Mongo::Cursor
    result = self.command(Commands::ListCollections, options: {
      filter:                 filter,
      name_only:              name_only,
      authorized_collections: authorized_collections,
    }).not_nil!
    Cursor.new(@client, result)
  end

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

  # Allows a client to observe all changes in a database.
  # Excludes system collections.
  # @returns a change stream on all collections in a database
  # Since: 4.0
  # See: https://docs.mongodb.com/manual/reference/system-collections/
  def watch(
    pipeline : Array = [] of BSON,
    *,
    full_document : String? = nil,
    resume_after = nil,
    max_await_time_ms : Int64? = nil,
    batch_size : Int32? = nil,
    collation : Collation? = nil,
    start_at_operation_time : Time? = nil,
    start_after = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil
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
    )
  end

  # Returns a variety of storage statistics for the database.
  def stats(*, scale : Int32? = nil) : BSON?
    self.command(Commands::DbStats, options: {scale: scale})
  end
end
