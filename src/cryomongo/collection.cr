require "./client"
require "./database"
require "./cursor"
require "./bulk"
require "./tools"
require "./concerns"
require "./read_preference"
require "./collation"
require "./index"
require "./change_stream"

# A `Collection` provides access to a MongoDB collection.
#
# ```
# collection = client["database_name"]["collection_name"]
# ```
class Mongo::Collection
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  # A collection name can be a String or an Integer.
  alias CollectionKey = String | Int32

  # The parent database.
  getter database : Mongo::Database
  # The collection name.
  getter name : CollectionKey

  # :nodoc:
  def initialize(@database, @name); end

  # Execute a command on the server targeting the collection.
  #
  # Will automatically set the *collection* and *database* arguments.
  #
  # See: `Mongo::Client.command`
  def command(
    operation,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil,
    **args,
    &block
  )
    @database.command(
      operation,
      **args,
      collection: @name,
      write_concern: write_concern || @write_concern,
      read_concern: read_concern || @read_concern,
      read_preference: read_preference || @read_preference,
      session: session
    ) { |result|
      yield result
    }
  end

  # :ditto:
  def command(
    operation,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil,
    **args
  )
    self.command(
      operation,
      **args,
      write_concern: write_concern,
      read_concern: read_concern,
      read_preference: read_preference,
      session: session,
    ) { |result| result }
  end

  # Runs an aggregation framework pipeline.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/aggregate/).
  def aggregate(
    pipeline : Array,
    *,
    allow_disk_use : Bool? = nil,
    batch_size : Int32? = nil,
    max_time_ms : Int64? = nil,
    bypass_document_validation : Bool? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    comment : String? = nil,
    read_concern : ReadConcern? = nil,
    write_concern : WriteConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::Cursor? forall H
    self.command(Commands::Aggregate, pipeline: pipeline, session: session, options: {
      allow_disk_use:             allow_disk_use,
      cursor:                     batch_size.try { {batchSize: batch_size} },
      bypass_document_validation: bypass_document_validation,
      collation:                  collation,
      hint:                       hint.is_a?(String) ? hint : BSON.new(hint),
      comment:                    comment,
      read_concern:               read_concern,
      write_concern:              write_concern,
      read_preference:            read_preference,
    }) { |result|
      Cursor.new(@database.client, result, batch_size: batch_size, session: session)
    }
  end

  # Count the number of documents in a collection that match the given filter.
  # Note that an empty filter will force a scan of the entire collection.
  # For a fast count of the total documents in a collection see `estimated_document_count`.
  #
  # See: [the specification document](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#count-api-details).
  def count_documents(
    filter = BSON.new,
    *,
    skip : Int32? = nil,
    limit : Int32? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    max_time_ms : Int64? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Int32 forall H
    pipeline = !filter || filter.empty? ? [BSON.new({"$match": BSON.new})] : [BSON.new({"$match": BSON.new(filter)})]
    skip.try { pipeline << BSON.new({"$skip": skip}) }
    limit.try { pipeline << BSON.new({"$limit": limit}) }
    pipeline << BSON.new({"$group": {"_id": 1, "n": {"$sum": 1}}})
    cursor = self.command(Commands::Aggregate, pipeline: pipeline, session: session, options: {
      collation:       collation,
      hint:            hint.is_a?(String) ? hint : BSON.new(hint),
      max_time_ms:     max_time_ms,
      read_preference: read_preference,
    }) { |result|
      Cursor.new(@database.client, result, limit: limit, session: session)
    }
    if (item = cursor.try(&.next)).is_a? BSON
      item["n"].as(Int32)
    else
      0
    end
  end

  # Gets an estimate of the count of documents in a collection using collection metadata.
  #
  # See: [the specification document](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#count-api-details).
  def estimated_document_count(*, max_time_ms : Int64? = nil, read_preference : ReadPreference? = nil, session : Session::ClientSession? = nil) : Int32
    result = self.command(Commands::Count, session: session, options: {
      max_time_ms:     max_time_ms,
      read_preference: read_preference,
    }).not_nil!
    result["n"].as(Int32)
  end

  # Finds the distinct values for a specified field across a single collection.
  #
  # NOTE: the results are backed by the "values" array in the distinct command's result
  # document. This differs from aggregate and find, where results are backed by a cursor.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/distinct/).
  def distinct(
    key : String,
    *,
    filter = nil,
    read_concern : ReadConcern? = nil,
    collation : Collation? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Array
    result = self.command(Commands::Distinct, key: key, session: session, options: {
      query:           filter,
      read_concern:    read_concern,
      collation:       collation,
      read_preference: read_preference,
    }).not_nil!
    result.values.each.map(&.[1]).to_a
  end

  # Finds the documents matching the model.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/find/).
  # NOTE: [for an overview of read operations, check the official manual](https://docs.mongodb.com/manual/core/read-operations-introduction/).
  def find(
    filter = BSON.new,
    *,
    sort = nil,
    projection = nil,
    hint : (String | H)? = nil,
    skip : Int32? = nil,
    limit : Int32? = nil,
    batch_size : Int32? = nil,
    single_batch : Bool? = nil,
    comment : String? = nil,
    max_time_ms : Int64? = nil,
    read_concern : ReadConcern? = nil,
    max = nil,
    min = nil,
    return_key : Bool? = nil,
    show_record_id : Bool? = nil,
    tailable : Bool? = nil,
    oplog_replay : Bool? = nil,
    no_cursor_timeout : Bool? = nil,
    await_data : Bool? = nil,
    allow_partial_results : Bool? = nil,
    allow_disk_use : Bool? = nil,
    collation : Collation? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::Cursor forall H
    self.command(Commands::Find, filter: filter, session: session, options: {
      sort:                  sort.try { BSON.new(sort) },
      projection:            projection.try { BSON.new(projection) },
      hint:                  hint.is_a?(String) ? hint : BSON.new(hint),
      skip:                  skip,
      limit:                 limit,
      batch_size:            batch_size,
      single_batch:          single_batch,
      comment:               comment,
      max_time_ms:           max_time_ms,
      read_concern:          read_concern,
      max:                   max.try { BSON.new(max) },
      min:                   min.try { BSON.new(min) },
      return_key:            return_key,
      show_record_id:        show_record_id,
      tailable:              tailable,
      oplog_replay:          oplog_replay,
      no_cursor_timeout:     no_cursor_timeout,
      await_data:            await_data,
      allow_partial_results: allow_partial_results,
      allow_disk_use:        allow_disk_use,
      collation:             collation,
      read_preference:       read_preference,
    }) { |result|
      Cursor.new(
        @database.client,
        result,
        await_time_ms: tailable && await_data ? max_time_ms : nil,
        tailable: tailable || false,
        batch_size: batch_size,
        limit: limit,
        session: session
      )
  }.not_nil!
  end

  # Finds the document matching the model.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/find/).
  def find_one(
    filter = BSON.new,
    *,
    sort = nil,
    projection = nil,
    hint : (String | H)? = nil,
    skip : Int32? = nil,
    comment : String? = nil,
    max_time_ms : Int64? = nil,
    read_concern : ReadConcern? = nil,
    max = nil,
    min = nil,
    return_key : Bool? = nil,
    show_record_id : Bool? = nil,
    oplog_replay : Bool? = nil,
    no_cursor_timeout : Bool? = nil,
    allow_partial_results : Bool? = nil,
    collation : Collation? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : BSON? forall H
    cursor = self.find(
      filter: filter,
      limit: 1,
      single_batch: true,
      batch_size: 1,
      tailable: false,
      await_data: false,
      sort: sort.try { BSON.new(sort) },
      projection: projection.try { BSON.new(projection) },
      hint: hint.is_a?(String) ? hint : BSON.new(hint),
      skip: skip,
      comment: comment,
      max_time_ms: max_time_ms,
      read_concern: read_concern,
      max: max.try { BSON.new(max) },
      min: min.try { BSON.new(min) },
      return_key: return_key,
      show_record_id: show_record_id,
      oplog_replay: oplog_replay,
      no_cursor_timeout: no_cursor_timeout,
      allow_partial_results: allow_partial_results,
      collation: collation,
      read_preference: read_preference,
      session: session
    )
    element = cursor.try &.next
    return element if element.is_a? BSON
    nil
  end

  # Executes multiple write operations.
  #
  # An error will be raised if the *requests* parameter is empty.
  #
  # NOTE: [for more details, please check the official specifications document](https://github.com/mongodb/specifications/blob/master/source/driver-bulk-update.rst).
  def bulk_write(requests : Array(Bulk::WriteModel), *, ordered : Bool, bypass_document_validation : Bool? = nil, session : Session::ClientSession? = nil) : Bulk::WriteResult
    raise Mongo::Bulk::Error.new "Tried to execute an empty bulk" unless requests.size > 0
    bulk = Mongo::Bulk.new(self, ordered, requests, session: session)
    bulk.execute(bypass_document_validation: bypass_document_validation)
  end

  # Create a `Mongo::Bulk` instance.
  def bulk(ordered : Bool = true, session : Session::ClientSession? = nil)
    Mongo::Bulk.new(self, ordered, session: session)
  end

  # Inserts the provided document. If the document is missing an identifier, it will be generated.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/insert/).
  def insert_one(document, *, write_concern : WriteConcern? = nil, bypass_document_validation : Bool? = nil, session : Session::ClientSession? = nil) : Commands::Common::InsertResult?
    self.command(Commands::Insert, documents: [document], session: session, options: {
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
      ordered:                    true,
    })
  end

  # Inserts the provided document. If any documents are missing an identifier, they will be generated.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/insert/).
  def insert_many(
    documents : Array,
    *,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    bypass_document_validation : Bool? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::Common::InsertResult?
    raise Mongo::Error.new "Tried to insert an empty document array" unless documents.size > 0
    self.command(Commands::Insert, documents: documents, session: session, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Deletes one document.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/delete/).
  def delete_one(
    filter,
    *,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::Common::DeleteResult? forall H
    delete = Tools.merge_bson({
      q:     BSON.new(filter),
      limit: 1,
    }, {
      collation: collation,
      hint:      hint,
    })
    self.command(Commands::Delete, deletes: [delete], session: session, options: {
      ordered:       ordered,
      write_concern: write_concern,
    })
  end

  # Deletes multiple documents.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/delete/).
  def delete_many(
    filter,
    *,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::Common::DeleteResult? forall H
    delete = Tools.merge_bson({
      q:     BSON.new(filter),
      limit: 0,
    }, {
      collation: collation,
      hint:      hint,
    })
    self.command(Commands::Delete, deletes: [delete], session: session, options: {
      ordered:       ordered,
      write_concern: write_concern,
    })
  end

  private def validate_replacement!(replacement)
    replacement = BSON.new(replacement)
    first_element = replacement.each.next
    raise Mongo::Error.new "The replacement document must not be an array" if replacement.is_a? Array
    unless first_element.is_a? Iterator::Stop
      if first_element[0].starts_with? '$'
        raise Mongo::Error.new "The replacement document parameter must not begin with an atomic modifier"
      elsif first_element[0] == '0'
        raise Mongo::Error.new "The replacement document must not be an array"
      end
    end
    replacement
  end

  private def validate_update!(update)
    unless update.is_a? Array
      update = BSON.new(update)
      first_element = update.each.next
      unless first_element.is_a? Iterator::Stop
        unless first_element[0].starts_with? '$'
          raise Mongo::Error.new "The update document parameter must have only atomic modifiers"
        end
      end
    end
    update
  end

  # Replaces a single document.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/update/).
  def replace_one(
    filter,
    replacement,
    *,
    upsert : Bool = false,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    bypass_document_validation : Bool? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::Common::UpdateResult? forall H
    updates = [
      Tools.merge_bson({
        q:      BSON.new(filter),
        u:      validate_replacement!(replacement),
        multi:  false,
        upsert: upsert,
      }, {
        collation: collation,
        hint:      hint,
      }),
    ]
    self.command(Commands::Update, updates: updates, session: session, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Updates one document.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/update/).
  def update_one(
    filter,
    update,
    *,
    upsert : Bool = false,
    array_filters = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    bypass_document_validation : Bool? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::Common::UpdateResult? forall H
    updates = [
      Tools.merge_bson({
        q:      BSON.new(filter),
        u:      validate_update!(update),
        multi:  false,
        upsert: upsert,
      }, {
        array_filters: array_filters,
        collation:     collation,
        hint:          hint,
      }),
    ]
    self.command(Commands::Update, updates: updates, session: session, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Updates multiple documents.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/update/).
  def update_many(
    filter,
    update,
    *,
    upsert : Bool = false,
    array_filters = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    bypass_document_validation : Bool? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::Common::UpdateResult? forall H
    updates = [
      Tools.merge_bson({
        q:      BSON.new(filter),
        u:      validate_update!(update),
        multi:  true,
        upsert: upsert,
      }, {
        array_filters: array_filters,
        collation:     collation,
        hint:          hint,
      }),
    ]
    self.command(Commands::Update, updates: updates, session: session, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  private def check_find_and_modify_result!(result)
    result = result.not_nil!
    if last_error_object = result["last_error_object"]?
      last_error_object = last_error_object.as(BSON)
      code = last_error_object["code"]?
      err_msg = last_error_object["errmsg"]?
      raise Mongo::Error::Command.new(code, err_msg)
    end

    result["value"]?.try &.as(BSON)
  end

  # Finds a single document and deletes it, returning the original. The document to return may be nil.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/findAndModify/).
  def find_one_and_delete(
    filter,
    *,
    sort = nil,
    fields = nil,
    bypass_document_validation : Bool? = nil,
    write_concern : WriteConcern? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    max_time_ms : Int64? = nil,
    session : Session::ClientSession? = nil
  ) : BSON? forall H
    result = self.command(Commands::FindAndModify, filter: filter, session: session, options: {
      remove:                     true,
      sort:                       sort.try { BSON.new(sort) },
      fields:                     fields.try { BSON.new(fields) },
      bypass_document_validation: bypass_document_validation,
      write_concern:              write_concern,
      collation:                  collation,
      hint:                       hint,
      max_time_ms:                max_time_ms,
    })
    check_find_and_modify_result!(result)
  end

  # Finds a single document and replaces it, returning either the original or the replaced
  # document. The document to return may be nil.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/findAndModify/).
  def find_one_and_replace(
    filter,
    replacement,
    *,
    sort = nil,
    new : Bool? = nil,
    fields = nil,
    upsert : Bool? = nil,
    bypass_document_validation : Bool? = nil,
    write_concern : WriteConcern? = nil,
    collation : Collation? = nil,
    array_filters = nil,
    hint : (String | H)? = nil,
    max_time_ms : Int64? = nil,
    session : Session::ClientSession? = nil
  ) : BSON? forall H
    replacement = validate_replacement!(replacement)
    result = self.command(Commands::FindAndModify, filter: filter, session: session, options: {
      update:                     replacement,
      sort:                       sort.try { BSON.new(sort) },
      new:                        new,
      fields:                     fields.try { BSON.new(fields) },
      upsert:                     upsert,
      bypass_document_validation: bypass_document_validation,
      write_concern:              write_concern,
      collation:                  collation,
      array_filters:              array_filters,
      hint:                       hint,
      max_time_ms:                max_time_ms,
    })
    check_find_and_modify_result!(result)
  end

  # Finds a single document and updates it, returning either the original or the updated
  # document. The document to return may be nil.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/findAndModify/).
  def find_one_and_update(
    filter,
    update,
    *,
    sort = nil,
    new : Bool? = nil,
    fields = nil,
    upsert : Bool? = nil,
    bypass_document_validation : Bool? = nil,
    write_concern : WriteConcern? = nil,
    collation : Collation? = nil,
    array_filters = nil,
    hint : (String | H)? = nil,
    max_time_ms : Int64? = nil,
    session : Session::ClientSession? = nil
  ) : BSON? forall H
    update = validate_update!(update)
    result = self.command(Commands::FindAndModify, filter: filter, session: session, options: {
      update:                     update,
      sort:                       sort.try { BSON.new(sort) },
      new:                        new,
      fields:                     fields.try { BSON.new(fields) },
      upsert:                     upsert,
      bypass_document_validation: bypass_document_validation,
      write_concern:              write_concern,
      collation:                  collation,
      array_filters:              array_filters,
      hint:                       hint,
      max_time_ms:                max_time_ms,
    })
    check_find_and_modify_result!(result)
  end

  # This is a convenience method for creating a single index.
  #
  # See: `create_indexes`
  def create_index(
    keys,
    *,
    options = NamedTuple.new,
    commit_quorum : (Int32 | String)? = nil,
    max_time_ms : Int64? = nil,
    write_concern : WriteConcern? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::CreateIndexes::Result?
    self.create_indexes(
      models: [{
        keys:    keys,
        options: options,
      }],
      commit_quorum: commit_quorum,
      max_time_ms: max_time_ms,
      write_concern: write_concern,
      session: session
    )
  end

  # Creates multiple indexes in the collection.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/createIndexes/).
  def create_indexes(
    models : Array,
    *,
    commit_quorum : (Int32 | String)? = nil,
    max_time_ms : Int64? = nil,
    write_concern : WriteConcern? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::CreateIndexes::Result?
    indexes = models.map { |item|
      if item.is_a? BSON
        keys = item["keys"].as(BSON)
        options = item["options"]?.try(&.as(BSON)) || BSON.new
        if options.["name"]?
          BSON.new({key: keys}).append(options)
        else
          index_name = keys.reduce([] of String) { |acc, (k, v)|
            acc << "#{k}_#{v}"
          }.join("_")
          options.append(name: index_name)
          BSON.new({key: keys}).append(options)
        end
      else
        index_model = Index::Model.new(item["keys"], Index::Options.new(**item["options"]))
        index_model.options.name = index_model.keys.reduce([] of String) { |acc, (k, v)|
          acc << "#{k}_#{v}"
        }.join("_") unless index_model.options.name
        BSON.new({key: index_model.keys}).append(index_model.options.to_bson)
      end
    }
    self.command(Commands::CreateIndexes, indexes: indexes, session: session, options: {
      commit_quorum: commit_quorum,
      max_time_ms:   max_time_ms,
      write_concern: write_concern,
    })
  end

  # Drops a single index from the collection by the index name.
  #
  # See: `drop_indexes`
  def drop_index(name : String, *, max_time_ms : Int64? = nil, write_concern : WriteConcern? = nil, session : Session::ClientSession? = nil) : Commands::Common::BaseResult?
    raise Mongo::Error.new "'*' cannot be used with drop_index as more than one index would be dropped." if name == "*"
    self.command(Commands::DropIndexes, index: name, session: session, options: {
      max_time_ms:   max_time_ms,
      write_concern: write_concern,
    })
  end

  # Drops all indexes in the collection.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/dropIndexes/).
  def drop_indexes(*, max_time_ms : Int64? = nil, write_concern : WriteConcern? = nil, session : Session::ClientSession? = nil) : Commands::Common::BaseResult?
    self.command(Commands::DropIndexes, index: "*", session: session, options: {
      max_time_ms:   max_time_ms,
      write_concern: write_concern,
    })
  end

  # Gets index information for all indexes in the collection.
  #
  # NOTE: [for more details, please check the official documentation](https://docs.mongodb.com/manual/reference/command/listIndexes/).
  def list_indexes(session : Session::ClientSession? = nil) : Mongo::Cursor?
    self.command(Commands::ListIndexes, session: session) { |result|
      Cursor.new(@database.client, result, session: session)
    }.not_nil!
  end

  # Returns a `ChangeStream::Cursor` watching a specific collection.
  #
  # ```
  # client = Mongo::Client.new
  # collection = client["db"]["coll"]
  #
  # spawn {
  #   cursor = collection.watch(
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
  #   collection.insert_one({count: i})
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
    start_at_operation_time : Time? = nil,
    resume_after : BSON? = nil,
    start_after : BSON? = nil,
    max_await_time_ms : Int64? = nil,
    batch_size : Int32? = nil,
    collation : Collation? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::ChangeStream::Cursor
    ChangeStream::Cursor.new(
      client: @database.client,
      database: @database.name,
      collection: name,
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

  # Returns a variety of storage statistics for the collection.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/collStats/).
  def stats(*, scale : Int32? = nil, session : Session::ClientSession? = nil) : BSON?
    self.command(Commands::CollStats, session: session, options: {scale: scale})
  end

  private class SessionProxy
    def initialize(@collection : Collection, @session : Session::ClientSession); end

    macro method_missing(call)
      @collection.{{call.name.id}}({% for arg in call.args %}{{arg}},{% end %}session: @session)
    end
  end

  # Initialize a session that has the same lifetime as the block.
  #
  # - First block argument is a reflection of the Collection instance with the *session* method argument already provided.
  # - Second block argument is the ClientSession.
  #
  # ```
  # client = Mongo::Client.new
  # collection = client["db"]["coll"]
  #
  # collection.with_session(causal_consistency: true) do |collection, session|
  #   5.times { |idx|
  #     # No need to provide: `session: session`.
  #     collection.insert_one({number: idx})
  #     collection.find_one({number: idx})
  #   }
  # end
  # ```
  def with_session(**args, &block)
    session = @database.client.start_session(**args)
    yield SessionProxy.new(self, session), session
  ensure
    session.try &.end
  end
end
