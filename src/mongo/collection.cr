require "./client"
require "./database"
require "./cursor"
require "./bulk"
require "./tools"
require "./concerns"
require "./read_preference"
require "./collation"
require "./index"

class Mongo::Collection
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  alias CollectionKey = String | Int32

  getter database : Mongo::Database
  getter name : CollectionKey

  def initialize(@database, @name); end

  def command(operation, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, read_preference : ReadPreference? = @read_preference, **args)
    @database.command(operation, **args, collection: @name, write_concern: write_concern, read_concern: read_concern, read_preference: read_preference)
  end

  #  Runs an aggregation framework pipeline.
  #
  #  Note: $out and $merge are special pipeline stages that cause no results
  #  to be returned from the server. As such, the iterable here would never
  #  contain documents. Drivers MAY setup a cursor to be executed upon
  #  iteration against the output collection such that if a user were to
  #  iterate the return value, results would be returned.
  #
  #  Note: result iteration should be backed by a cursor. Depending on the implementation,
  #  the cursor may back the returned Iterable instance or an iterator that it produces.
  #
  #  See: https://docs.mongodb.com/manual/reference/command/aggregate/
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
    write_concern : WriteConcern? = nil,
    read_preference : ReadPreference? = nil
  ) : Mongo::Cursor? forall H
    maybe_result = self.command(Commands::Aggregate, pipeline: pipeline, options: {
      allow_disk_use:             allow_disk_use,
      cursor:                     batch_size.try { {batchSize: batch_size} },
      bypass_document_validation: bypass_document_validation,
      read_concern:               read_concern,
      collation:                  collation,
      hint:                       hint.is_a?(String) ? hint : BSON.new(hint),
      comment:                    comment,
      write_concern:              write_concern,
      read_preference:            read_preference,
    })
    maybe_result.try { |result| Cursor.new(@database.client, result) }
  end

  # Count the number of documents in a collection that match the given
  # filter. Note that an empty filter will force a scan of the entire
  # collection. For a fast count of the total documents in a collection
  # see `estimatedDocumentCount`.
  #
  # See: https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#count-api-details
  def count_documents(
    filter = BSON.new,
    *,
    skip : Int32? = nil,
    limit : Int32? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    max_time_ms : Int32? = nil,
    read_preference : ReadPreference? = nil
  ) : Int32 forall H
    pipeline = [BSON.new({"$match": filter})]
    skip.try { pipeline << BSON.new({"$skip": skip}) }
    limit.try { pipeline << BSON.new({"$limit": limit}) }
    pipeline << BSON.new({"$group": {"_id": 1, "n": {"$sum": 1}}})
    result = self.command(Commands::Aggregate, pipeline: pipeline, options: {
      collation:       collation,
      hint:            hint.is_a?(String) ? hint : BSON.new(hint),
      max_time_ms:     max_time_ms,
      read_preference: read_preference,
    }).not_nil!
    cursor = Cursor.new(@database.client, result)
    if (item = cursor.next).is_a? BSON
      item["n"].as(Int32)
    else
      0
    end
  end

  # Gets an estimate of the count of documents in a collection using collection metadata.
  #
  # See: https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#count-api-details
  def estimated_document_count(*, max_time_ms : Int64? = nil, read_preference : ReadPreference? = nil) : Int32
    result = self.command(Commands::Count, options: {
      max_time_ms:     max_time_ms,
      read_preference: read_preference,
    }).not_nil!
    result["n"].as(Int32)
  end

  # Finds the distinct values for a specified field across a single collection.
  #
  # Note: the results are backed by the "values" array in the distinct command's result
  # document. This differs from aggregate and find, where results are backed by a cursor.
  #
  # See: https://docs.mongodb.com/manual/reference/command/distinct/
  def distinct(
    key : String,
    *,
    filter = nil,
    read_concern : ReadConcern? = nil,
    collation : Collation? = nil,
    read_preference : ReadPreference? = nil
  ) : Array
    result = self.command(Commands::Distinct, key: key, options: {
      query:           filter,
      read_concern:    read_concern,
      collation:       collation,
      read_preference: read_preference,
    }).not_nil!
    result.values.each.map(&.[1]).to_a
  end

  # Finds the documents matching the model.
  #
  # Note: The filter parameter below equates to the $query meta operator. It cannot
  # contain other meta operators like $maxScan. However, do not validate this document
  # as it would be impossible to be forwards and backwards compatible. Let the server
  # handle the validation.
  #
  # Note: If $explain is specified in the modifiers, the return value is a single
  # document. This could cause problems for static languages using strongly typed entities.
  #
  # Note: result iteration should be backed by a cursor. Depending on the implementation,
  # the cursor may back the returned Iterable instance or an iterator that it produces.
  #
  # See: https://docs.mongodb.com/manual/reference/command/find/
  # See: https://docs.mongodb.com/manual/core/read-operations-introduction/
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
    collation : Collation? = nil,
    read_preference : ReadPreference? = nil
  ) : Mongo::Cursor forall H
    result = self.command(Commands::Find, filter: filter, options: {
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
      collation:             collation,
      read_preference:       read_preference,
    }).not_nil!
    Cursor.new(@database.client, result, await_time_ms: tailable && await_data ? max_time_ms : nil)
  end

  # Finds the document matching the model.
  #
  # Note: The filter parameter below equates to the $query meta operator. It cannot
  # contain other meta operators like $maxScan. However, do not validate this document
  # as it would be impossible to be forwards and backwards compatible. Let the server
  # handle the validation.
  #
  # See: https://docs.mongodb.com/manual/reference/command/find/
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
    read_preference : ReadPreference? = nil
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
      read_preference: read_preference
    ).not_nil!
    element = cursor.next
    return element if element.is_a? BSON
    nil
  end

  # Executes multiple write operations.
  #
  # An error MUST be raised if the requests parameter is empty.
  #
  # For servers < 3.4, if a collation was explicitly set for any request, an error MUST be raised
  # and no documents sent.
  #
  # NOTE: see the FAQ about the previous bulk API and how it relates to this.
  # See: https://docs.mongodb.com/manual/reference/command/delete/
  # See: https://docs.mongodb.com/manual/reference/command/insert/
  # See: https://docs.mongodb.com/manual/reference/command/update/
  # @throws InvalidArgumentException if requests is empty
  # @throws BulkWriteException
  def bulk_write(requests : Array(Bulk::WriteModel), *, ordered : Bool, bypass_document_validation : Bool? = nil) : Bulk::WriteResult
    raise "Tried to execute an empty bulk" unless requests.size > 0
    bulk = Mongo::Bulk.new(self, ordered, requests)
    bulk.execute(bypass_document_validation: bypass_document_validation)
  end

  def bulk(ordered : Bool = true)
    Mongo::Bulk.new(self, ordered)
  end

  # Inserts the provided document. If the document is missing an identifier,
  # the driver should generate one.
  #
  # See: https://docs.mongodb.com/manual/reference/command/insert/
  # @throws WriteException
  def insert_one(document, *, write_concern : WriteConcern? = nil, bypass_document_validation : Bool? = nil) : Commands::Common::InsertResult?
    self.command(Commands::Insert, documents: [document], options: {
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Inserts the provided documents. If any documents are missing an identifier,
  # the driver should generate them.
  #
  # An error MUST be raised if the documents parameter is empty.
  #
  # Note that this uses the bulk insert command underneath and should not
  # use OP_INSERT.
  #
  # See: https://docs.mongodb.com/manual/reference/command/insert/
  # @throws InvalidArgumentException if documents is empty
  # @throws BulkWriteException
  def insert_many(
    documents : Array,
    *,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    bypass_document_validation : Bool? = nil
  ) : Commands::Common::InsertResult?
    raise "Tried to insert an empty document array" unless documents.size > 0
    self.command(Commands::Insert, documents: documents, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Deletes one document.
  #
  # See: https://docs.mongodb.com/manual/reference/command/delete/
  # @throws WriteException
  def delete_one(
    filter,
    *,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil
  ) : Commands::Common::DeleteResult? forall H
    delete = Tools.merge_bson({
      q:     BSON.new(filter),
      limit: 1,
    }, {
      collation: collation,
      hint:      hint,
    })
    self.command(Commands::Delete, deletes: [delete], options: {
      ordered:       ordered,
      write_concern: write_concern,
    })
  end

  # Deletes multiple documents.
  #
  # See: https://docs.mongodb.com/manual/reference/command/delete/
  # @throws WriteException
  def delete_many(
    filter,
    *,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil
  ) : Commands::Common::DeleteResult? forall H
    delete = Tools.merge_bson({
      q:     BSON.new(filter),
      limit: 0,
    }, {
      collation: collation,
      hint:      hint,
    })
    self.command(Commands::Delete, deletes: [delete], options: {
      ordered:       ordered,
      write_concern: write_concern,
    })
  end

  private def validate_replacement!(replacement)
    replacement = BSON.new(replacement)
    first_element = replacement.each.next
    raise "The replacement document must not be an array" if replacement.is_a? Array
    unless first_element.is_a? Iterator::Stop
      if first_element[0].starts_with? '$'
        raise "The replacement document parameter must not begin with an atomic modifier"
      elsif first_element[0] == '0'
        raise "The replacement document must not be an array"
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
          raise "The update document parameter must have only atomic modifiers"
        end
      end
    end
    update
  end

  # Replaces a single document.
  #
  # See: https://docs.mongodb.com/manual/reference/command/update/
  # @throws WriteException
  def replace_one(
    filter,
    replacement,
    *,
    upsert : Bool = false,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    ordered : Bool? = nil,
    write_concern : WriteConcern? = nil,
    bypass_document_validation : Bool? = nil
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
    self.command(Commands::Update, updates: updates, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Updates one document.
  #
  # See: https://docs.mongodb.com/manual/reference/command/update/
  # @throws WriteException
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
    bypass_document_validation : Bool? = nil
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
    self.command(Commands::Update, updates: updates, options: {
      ordered:                    ordered,
      write_concern:              write_concern,
      bypass_document_validation: bypass_document_validation,
    })
  end

  # Updates multiple documents.
  #
  # See: https://docs.mongodb.com/manual/reference/command/update/
  # @throws WriteException
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
    bypass_document_validation : Bool? = nil
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
    self.command(Commands::Update, updates: updates, options: {
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
      raise Mongo::CommandError.new(code, err_msg)
    end

    result["value"]?.try &.as(BSON)
  end

  # Finds a single document and deletes it, returning the original. The document to return may be null.
  #
  # See: https://docs.mongodb.com/manual/reference/command/findAndModify/
  # @throws WriteException
  def find_one_and_delete(
    filter,
    *,
    sort = nil,
    new : Bool? = nil,
    fields = nil,
    bypass_document_validation : Bool? = nil,
    write_concern : WriteConcern? = nil,
    collation : Collation? = nil,
    hint : (String | H)? = nil,
    max_time_ms : Int32? = nil,
  ) : BSON? forall H
    result = self.command(Commands::FindAndModify, filter: filter, options: {
      remove:                     true,
      sort:                       sort.try { BSON.new(sort) },
      new:                        new,
      fields:                     fields.try { BSON.new(fields) },
      bypass_document_validation: bypass_document_validation,
      write_concern:              write_concern,
      collation:                  collation,
      hint:                       hint,
      max_time_ms:                max_time_ms
    })
    check_find_and_modify_result!(result)
  end

  # Finds a single document and replaces it, returning either the original or the replaced
  # document. The document to return may be null.
  #
  # See: https://docs.mongodb.com/manual/reference/command/findAndModify/
  # @throws WriteException
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
    max_time_ms : Int32? = nil,
  ) : BSON? forall H
    replacement = validate_replacement!(replacement)
    result = self.command(Commands::FindAndModify, filter: filter, options: {
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
      max_time_ms:                max_time_ms
    })
    check_find_and_modify_result!(result)
  end

  # Finds a single document and updates it, returning either the original or the updated
  # document. The document to return may be null.
  #
  # See: https://docs.mongodb.com/manual/reference/command/findAndModify/
  # @throws WriteException
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
    max_time_ms : Int32? = nil,
  ) : BSON? forall H
    update = validate_update!(update)
    result = self.command(Commands::FindAndModify, filter: filter, options: {
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
      max_time_ms:                max_time_ms
    })
    check_find_and_modify_result!(result)
  end

  # This is a convenience method for creating a single index. This MUST call the
  # createIndexes method and pass the provided specification document in a
  # sequence to that method with the same options.
  #
  # @return The name of the created index.
  #
  # Note: Drivers MAY opt to implement this method signature, the signature that
  #   takes an IndexModel as a parameter, or for those languages with method
  #   overloading MAY decide to implement both.
  #
  # Note: Drivers MAY combine the two options types into a single one. If the options are
  #   explicitly typed, the combined options type MUST be named CreateIndexOptions or an acceptable
  #   variation.
  def create_index(
    keys,
    *,
    options = NamedTuple.new,
    commit_quorum : (Int32 | String)? = nil,
    max_time_ms : Int64? = nil,
    write_concern : WriteConcern? = nil,
  ) : Commands::Common::BaseResult?
    self.create_indexes(
      models: [{
        keys: keys,
        options: options
      }],
      commit_quorum: commit_quorum,
      max_time_ms: max_time_ms,
      write_concern: write_concern
    )
  end

  # Creates multiple indexes in the collection.
  #
  # For MongoDB 2.6 and higher this method MUST execute a createIndexes command.
  #
  # For MongoDB 2.4 this method MUST insert the index specifications directly into
  # the system.indexes collection. The write concern provided provided to the server
  # MUST be { w: 1 }.
  #
  # The driver MAY choose NOT to support creating indexes on 2.4 and if so, MUST
  # document the method as such.
  #
  # Note that in MongoDB server versions >= 3.0.0, the server will create the
  # indexes in parallel.
  #
  # As of 3.4 (see https://jira.mongodb.org/browse/SERVER-769) the server validates
  # options passed to the createIndexes command.
  #
  # @return The names of all the indexes that were created.
  def create_indexes(
    models : Array,
    *,
    commit_quorum : (Int32 | String)? = nil,
    max_time_ms : Int64? = nil,
    write_concern : WriteConcern? = nil,
  ) : Commands::Common::BaseResult?
    indexes = models.map { |item|
      index_model = IndexModel.new(item["keys"], IndexOptions.new(**item["options"]))
      index_model.options.name = index_model.keys.reduce([] of String) { |acc, (k, v)|
        acc << "#{k}_#{v}"
      }.join("_") unless index_model.options.name
      BSON.new({ key: index_model.keys }).append(index_model.options.to_bson)
    }
    self.command(Commands::CreateIndexes, indexes: indexes, options: {
      commit_quorum: commit_quorum,
      max_time_ms: max_time_ms,
      write_concern: write_concern,
    })
  end

  # Drops a single index from the collection by the index name.
  #
  # In all server versions this MUST execute a dropIndexes command.
  #
  # Note: If the string passed is '*', the driver MUST raise an error since
  #   more than one index would be dropped.
  def drop_index(name : String, *, max_time_ms : Int64? = nil, write_concern : WriteConcern? = nil): Commands::Common::BaseResult?
    raise Mongo::Error.new "'*' cannot be used with drop_index as more than one index would be dropped." if name == "*"
    self.command(Commands::DropIndexes, index: name, options: {
      max_time_ms: max_time_ms,
      write_concern: write_concern,
    })
  end


  # Drops all indexes in the collection.
  def drop_indexes(*, max_time_ms : Int64? = nil, write_concern : WriteConcern? = nil) : Commands::Common::BaseResult?
    self.command(Commands::DropIndexes, index: "*", options: {
      max_time_ms: max_time_ms,
      write_concern: write_concern,
    })
  end

  # Gets index information for all indexes in the collection. This should be
  # implemented as specified in the Enumerate Indexes specification:
  #
  # See: https://github.com/mongodb/specifications/blob/master/source/enumerate-indexes.rst
  def list_indexes : Mongo::Cursor
    result = self.command(Commands::ListIndexes).not_nil!
    Cursor.new(@database.client, result)
  end
end
