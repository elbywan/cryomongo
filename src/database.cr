require "./client"
require "./collection"
require "./concerns"

class Mongo::Database
  include WithReadConcern
  include WithWriteConcern

  getter client : Mongo::Client
  getter name : String

  def initialize(@client, @name)
  end

  def command(operation, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, **args)
    @client.command(operation, **args, database: @name, write_concern: write_concern, read_concern: read_concern)
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
  def aggregate(pipeline : Array, **options) : Mongo::Cursor
    result = self.command(Commands::Aggregate, collection: 1, pipeline: pipeline, options: options).not_nil!
    Cursor.new(@client, result)
  end

  def list_collections(**options) : Mongo::Cursor
    result = self.command(Commands::ListCollections, options: options).not_nil!
    Cursor.new(@client, result)
  end
end
