# Change streams allow applications to access real-time data changes without the complexity and risk of tailing the oplog.
#
# Applications can use change streams to subscribe to all data changes on a single collection, a database, or an entire deployment,
# and immediately react to them. Because change streams use the aggregation framework, applications can also filter for specific changes
# or transform the notifications at will.
#
# NOTE: [for more details, please check the official manual](https://docs.mongodb.com/manual/changeStreams/index.html).
module Mongo::ChangeStream
  @[BSON::Options(camelize: "lower")]
  struct Document(T)
    include BSON::Serializable

    # The id functions as an opaque token for use when resuming an interrupted
    # change stream.
    getter _id : BSON

    # Describes the type of operation represented in this change notification.
    # "insert" | "update" | "replace" | "delete" | "invalidate" | "drop" | "dropDatabase" | "rename"
    getter operation_type : String

    # Contains two fields: “db” and “coll” containing the database and
    # collection name in which the change happened.
    getter ns : BSON

    # Only present for ops of type ‘insert’, ‘update’, ‘replace’, and
    # ‘delete’.
    #
    # For unsharded collections this contains a single field, _id, with the
    # value of the _id of the document updated.  For sharded collections,
    # this will contain all the components of the shard key in order,
    # followed by the _id if the _id isn’t part of the shard key.
    getter document_key : BSON? = nil

    # Only present for ops of type ‘update’.
    #
    # Contains a description of updated and removed fields in this
    # operation.
    getter update_description : UpdateDescription? = nil

    # Always present for operations of type ‘insert’ and ‘replace’. Also
    # present for operations of type ‘update’ if the user has specified ‘updateLookup’
    # in the ‘fullDocument’ arguments to the ‘$changeStream’ stage.
    #
    # For operations of type ‘insert’ and ‘replace’, this key will contain the
    # document being inserted, or the new version of the document that is replacing
    # the existing document, respectively.
    #
    # For operations of type ‘update’, this key will contain a copy of the full
    # version of the document from some point after the update occurred. If the
    # document was deleted since the updated happened, it will be null.
    getter full_document : T? = nil

    @[BSON::Options(camelize: "lower")]
    struct UpdateDescription
      include BSON::Serializable

      # A document containing key:value pairs of names of the fields that were
      # changed, and the new value for those fields.
      getter updated_fields : BSON

      # An array of field names that were removed from the document.
      getter removed_fields : Array(String)
    end
  end

  class Cursor < ::Mongo::Cursor
    # The resume_token can be used to create a change stream that will start from this cursor position.
    getter resume_token : BSON? = nil

    @options : NamedTuple(
      pipeline: Array(BSON),
      full_document: String?,
      start_at_operation_time: Time?,
      resume_after: BSON?,
      start_after: BSON?,
      max_time_ms: Int64?,
      batch_size: Int32?,
      collation: Collation?,
      read_concern: ReadConcern?,
      read_preference: ReadPreference?,
      collection: Collection::CollectionKey,
      database: String)

    # :nodoc:
    def initialize(@client : Mongo::Client, @session : Session::ClientSession? = nil, @limit : Int32? = nil, **@options)
      @await_time_ms = options["max_time_ms"]?
      @tailable = true
      @counter = 0

      @cursor_id = 0
      @batch_size = options["batch_size"]?
      @batch = [] of BSON
      @database = options["database"]
      @collection = options["collection"]

      result = init(**@options).not_nil!

      @cursor_id = result.cursor.id
      @batch = result.cursor.first_batch
      @database, @collection = result.cursor.ns.split(".", 2)
    end

    # Will convert the elements to the `Mongo::ChangeStream::Document(T)` type while iterating the `Cursor`.
    #
    # NOTE: see `Mongo::Cursor.of`
    def of(type : T) forall T
      {% begin %}
      Cursor::Wrapper(Mongo::ChangeStream::Document({{T.instance}})).new(self)
      {% end %}
    end

    def next : BSON | Iterator::Stop
      element = super
      if element.is_a?(BSON) && @batch.empty?
        @resume_token ||= element["_id"]?.try &.as(BSON)
      end
      element
    rescue e : Mongo::Error::Command
      # see: https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resume-process
      if e.resumable?
        self.close

        if resume_token
          result = init(
            **@options.merge({
              resume_after:            resume_token,
              start_after:             nil,
              start_at_operation_time: nil,
            })
          ).not_nil!
        else
          result = init(**@options).not_nil!
        end

        @cursor_id = result.cursor.id
        @batch = result.cursor.first_batch
        @database, @collection = result.cursor.ns.split(".", 2)
        self.next
      else
        raise e
      end
    end

    protected def fetch_more
      reply = super
      if reply
        @resume_token = reply.cursor.post_batch_resume_token
      end
      reply
    end

    private def init(
      pipeline : Array(BSON) = [] of BSON,
      full_document : String? = nil,
      start_at_operation_time : Time? = nil,
      resume_after : BSON? = nil,
      start_after : BSON? = nil,
      max_time_ms : Int64? = nil,
      batch_size : Int32? = nil,
      collation : Collation? = nil,
      read_concern : ReadConcern? = nil,
      read_preference : ReadPreference? = nil,
      collection : Collection::CollectionKey = nil,
      database : String = nil
    )
      full_pipeline = self.make_pipeline(
        pipeline: pipeline,
        full_document: full_document,
        resume_after: resume_after,
        start_after: start_after,
        start_at_operation_time: start_at_operation_time
      )

      @client.command(
        Commands::Aggregate,
        pipeline: full_pipeline,
        collection: collection,
        database: database,
        read_concern: read_concern,
        read_preference: read_preference,
        session: @session,
        options: {
          max_time_ms: max_time_ms,
          batch_size:  batch_size,
          collation:   collation,
        }
      )
    end

    private def make_pipeline(*, pipeline, full_document, resume_after, start_after, start_at_operation_time)
      change_stream_stage = Tools.merge_bson(NamedTuple.new, {
        full_document:           full_document,
        resume_after:            resume_after,
        start_at_operation_time: start_at_operation_time.try { |time| BSON::Timestamp.new(time.to_unix.to_u32, 1_u32) },
        start_after:             start_after,
        allChangesForCluster:    (@options["database"]? == "admin") || nil,
      })
      [
        BSON.new({
          "$changeStream": change_stream_stage,
        }),
      ].concat(pipeline)
    end
  end
end
