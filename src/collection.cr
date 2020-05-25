require "./client"
require "./database"
require "./cursor"
require "./bulk"
require "./tools"
require "./concerns"

class Mongo::Collection
  include WithReadConcern
  include WithWriteConcern

  alias CollectionKey = String | Int32

  getter database : Mongo::Database
  getter name : CollectionKey

  def initialize(@database, @name); end

  def command(operation, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, **args)
    @database.command(operation, **args, collection: @name, write_concern: write_concern, read_concern: read_concern)
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
  def aggregate(pipeline : Array, **options) : Mongo::Cursor
    # if concern = @read_concern
    #   options = options.merge({ read_concern: concern })
    # end
    # if concern = @write_concern
    #   options = options.merge({ write_concern: concern })
    # end

    result = self.command(Commands::Aggregate, pipeline: pipeline, options: options).not_nil!
    await_time_ms = options["tailable"]? && options["await_data"]? ? options["max_time_ms"]? : nil
    Cursor.new(@database.client, result, await_time_ms)
  end

  # Count the number of documents in a collection that match the given
  # filter. Note that an empty filter will force a scan of the entire
  # collection. For a fast count of the total documents in a collection
  # see `estimatedDocumentCount`.
  #
  # See: https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#count-api-details
  def count_documents(filter = BSON.new, skip : Int32? = nil, limit : Int32? = nil, **options) : Int32
    pipeline = [BSON.new({"$match": filter})]
    skip.try { pipeline << BSON.new({"$skip": skip}) }
    limit.try { pipeline << BSON.new({"$limit": limit}) }
    pipeline << BSON.new({"$group": {"_id": 1, "n": {"$sum": 1}}})
    result = self.command(Commands::Aggregate, pipeline: pipeline, options: options, write_concern: nil).not_nil!
    await_time_ms = options["tailable"]? && options["await_data"]? ? options["max_time_ms"]? : nil
    cursor = Cursor.new(@database.client, result, await_time_ms)
    if (item = cursor.next).is_a? BSON
      item["n"].as(Int32)
    else
      0
    end
  end

  # Gets an estimate of the count of documents in a collection using collection metadata.
  #
  # See: https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#count-api-details
  def estimated_document_count(max_time_ms : Int64? = nil) : Int32
    options = {max_time_ms: max_time_ms} unless max_time_ms.nil?
    result = self.command(Commands::Count, options: options).not_nil!
    result["n"].as(Int32)
  end

  # Finds the distinct values for a specified field across a single collection.
  #
  # Note: the results are backed by the "values" array in the distinct command's result
  # document. This differs from aggregate and find, where results are backed by a cursor.
  #
  # See: https://docs.mongodb.com/manual/reference/command/distinct/
  def distinct(field_name : String, filter = BSON.new, **options) : Array
    result = self.command(Commands::Distinct, key: field_name, options: options.merge({query: filter})).not_nil!
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
  def find(filter = BSON.new, **options) : Mongo::Cursor
    result = self.command(Commands::Find, filter: filter, options: options).not_nil!
    await_time_ms = options["tailable"]? && options["await_data"]? ? options["max_time_ms"]? : nil
    Cursor.new(@database.client, result, await_time_ms)
  end

  # Finds the document matching the model.
  #
  # Note: The filter parameter below equates to the $query meta operator. It cannot
  # contain other meta operators like $maxScan. However, do not validate this document
  # as it would be impossible to be forwards and backwards compatible. Let the server
  # handle the validation.
  #
  # See: https://docs.mongodb.com/manual/reference/command/find/
  def find_one(filter = BSON.new, **options) : BSON?
    cursor = self.find(**options, filter: filter, limit: 1, singleBatch: true, tailable: false, awaitData: false).not_nil!
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
  # @see https://docs.mongodb.com/manual/reference/command/delete/
  # @see https://docs.mongodb.com/manual/reference/command/insert/
  # @see https://docs.mongodb.com/manual/reference/command/update/
  # @throws InvalidArgumentException if requests is empty
  # @throws BulkWriteException
  def bulk_write(requests : Array(Bulk::WriteModel), ordered : Bool, bypass_document_validation : Bool? = nil) : Bulk::WriteResult
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
  # @see https://docs.mongodb.com/manual/reference/command/insert/
  # @throws WriteException
  def insert_one(document, **options) : Commands::Common::InsertResult?
    self.command(Commands::Insert, documents: [document], options: options)
  end

  # Inserts the provided documents. If any documents are missing an identifier,
  # the driver should generate them.
  #
  # An error MUST be raised if the documents parameter is empty.
  #
  # Note that this uses the bulk insert command underneath and should not
  # use OP_INSERT.
  #
  # @see https://docs.mongodb.com/manual/reference/command/insert/
  # @throws InvalidArgumentException if documents is empty
  # @throws BulkWriteException
  def insert_many(documents : Array, **options) : Commands::Common::InsertResult?
    raise "Tried to insert an empty document array" unless documents.size > 0
    self.command(Commands::Insert, documents: documents, options: options)
  end

  # Deletes one document.
  #
  # @see https://docs.mongodb.com/manual/reference/command/delete/
  # @throws WriteException
  def delete_one(filter, collation = nil, hint = nil, **options) : Commands::Common::DeleteResult?
    delete = Tools.merge_bson({
      q: BSON.new(filter),
      limit: 1,
    }, {
      collation: collation,
      hint: hint
    })
    self.command(Commands::Delete, deletes: [delete], options: options)
  end

  # Deletes multiple documents.
  #
  # @see https://docs.mongodb.com/manual/reference/command/delete/
  # @throws WriteException
  def delete_many(filter, collation = nil, hint = nil, **options): Commands::Common::DeleteResult?
    delete = Tools.merge_bson({
      q: BSON.new(filter),
      limit: 0,
    }, {
      collation: collation,
      hint: hint
    })
    self.command(Commands::Delete, deletes: [delete], options: options)
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
  # @see https://docs.mongodb.com/manual/reference/command/update/
  # @throws WriteException
  def replace_one(filter, replacement, upsert = false, collation = nil, hint = nil, **options) : Commands::Common::UpdateResult?
    updates = [
      Tools.merge_bson({
        q: BSON.new(filter),
        u: validate_replacement!(replacement),
        multi: false,
        upsert: upsert
      }, {
        collation: collation,
        hint: hint
      })
    ]
    self.command(Commands::Update, updates: updates, options: options)
  end

  # Updates one document.
  #
  # @see https://docs.mongodb.com/manual/reference/command/update/
  # @throws WriteException
  def update_one(filter, update, upsert = false, array_filters = nil, collation = nil, hint = nil, **options) : Commands::Common::UpdateResult?
    updates = [
      Tools.merge_bson({
        q: BSON.new(filter),
        u: validate_update!(update),
        multi: false,
        upsert: upsert
      }, {
        array_filters: array_filters,
        collation: collation,
        hint: hint
      })
    ]
    self.command(Commands::Update, updates: updates, options: options)
  end

  # Updates multiple documents.
  #
  # @see https://docs.mongodb.com/manual/reference/command/update/
  # @throws WriteException
  def update_many(filter, update, upsert = false, array_filters = nil, collation = nil, hint = nil, **options) : Commands::Common::UpdateResult?
    updates = [
      Tools.merge_bson({
        q: BSON.new(filter),
        u: validate_update!(update),
        multi: true,
        upsert: upsert
      }, {
        array_filters: array_filters,
        collation: collation,
        hint: hint
      })
    ]
    self.command(Commands::Update, updates: updates, options: options)
  end

  private def check_find_and_modify_result!(result)
    if last_error_object = result["last_error_object"]
      code = last_error_object["code"]?
      err_msg = last_error_object["errmsg"]?
      raise Mongo::Error.new(code, err_msg)
    end

    result["value"]?
  end

  # Finds a single document and deletes it, returning the original. The document to return may be null.
  #
  # @see https://docs.mongodb.com/manual/reference/command/findAndModify/
  # @throws WriteException
  def find_one_and_delete(filter, **options) : BSON
    raise "Update argument is disallowed" if options.has_key "update"
    self.command(Commands::FindAndModify, filter: filter, options: options, remove: true)
    check_find_and_modify_result!(result)
  end

  # Finds a single document and replaces it, returning either the original or the replaced
  # document. The document to return may be null.
  #
  # @see https://docs.mongodb.com/manual/reference/command/findAndModify/
  # @throws WriteException
  def find_one_and_replace(filter, replacement, **options) : BSON
    raise "Remove argument is disallowed" if options.has_key "remove"
    replacement = validate_replacement!(replacement)
    result = self.command(Commands::FindAndModify, filter: filter, options: options, update: replacement)
    check_find_and_modify_result!(result)
  end

  # Finds a single document and updates it, returning either the original or the updated
  # document. The document to return may be null.
  #
  # @see https://docs.mongodb.com/manual/reference/command/findAndModify/
  # @throws WriteException
  def find_one_and_update(filter, update, **options) : BSON
    raise "Remove argument is disallowed" if options.has_key "remove"
    update = validate_update!(update)
    result = self.command(Commands::FindAndModify, filter: filter, options: options, update: update)
    check_find_and_modify_result!(result)
  end

  def list_indexes
    result = self.command(Commands::ListIndexes).not_nil!
    Cursor.new(@database.client, result)
  end
end
