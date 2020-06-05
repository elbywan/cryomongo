require "./clients/client"
require "./collection"
require "./concerns"
require "./read_preference"

class Mongo::Database
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  getter client : Mongo::Client
  getter name : String

  def initialize(@client, @name)
  end

  def command(operation, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, read_preference : ReadPreference? = @read_preference, **args)
    @client.command(operation, **args, database: @name, write_concern: write_concern, read_concern: read_concern, read_preference: read_preference)
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
    max_time_ms : Int32? = nil,
    bypass_document_validation : Bool? = nil,
    read_concern : ReadConcern? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    comment : String? = nil,
    write_concern : WriteConcern? = nil
  ) : Mongo::Cursor? forall H
    maybe_result = self.command(Commands::Aggregate, collection: 1, pipeline: pipeline, options: {
      allow_disk_use: allow_disk_use,
      cursor: batch_size.try { { batch_size: batch_size } },
      bypass_document_validation: bypass_document_validation,
      read_concern: read_concern,
      collation: collation,
      hint: hint.is_a?(String) ? hint : BSON.new(hint),
      comment: comment,
      write_concern: write_concern
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
      filter: filter,
      name_only: name_only,
      authorized_collections: authorized_collections
    }).not_nil!
    Cursor.new(@client, result)
  end
end
