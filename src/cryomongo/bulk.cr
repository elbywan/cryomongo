require "./collection"
require "./tools"
require "./commands/**"

# A bulk operations builder.
#
# ```
# # A Bulk instance can be obtained by calling `.bulk()` on a collection.
# bulk = collection.bulk(ordered: true)
# # Then, operations can be added…
# 500.times { |idx|
#   bulk.insert_one({number: idx})
#   bulk.delete_many({number: {"$lt": 450}})
# }
# # …and they will be performed once the bulk gets executed.
# bulk_result = bulk.execute(write_concern: Mongo::WriteConcern.new(w: 1))
# pp bulk_result
# ```
struct Mongo::Bulk
  # The target collection.
  getter collection : Mongo::Collection
  # Whether the bulk is ordered.
  getter ordered : Bool

  @models = [] of WriteModel
  @max_bson_object_size : Int32 = 16 * 1024 * 1024
  @max_write_batch_size : Int32 = 100_000
  @executed = Atomic(UInt8).new(0)

  # :nodoc:
  def initialize(@collection, @ordered = true)
    # handshake_result = @collection.database.client.handshake_reply
    # @max_bson_object_size = handshake_result.max_bson_object_size
    # @max_write_batch_size = handshake_result.max_write_batch_size
  end

  # :nodoc:
  def initialize(collection, ordered, @models)
    initialize(collection, ordered)
  end

  # The base Struct inherited by all the bulk write models.
  abstract struct WriteModel
    def <=>(other)
      self.class.to_s <=> other.class.to_s
    end
  end

  # Insert one document.
  struct InsertOne < WriteModel
    getter document : BSON

    def initialize(document)
      @document = BSON.new(document)
    end
  end

  # Delete one document.
  struct DeleteOne < WriteModel
    getter filter : BSON
    getter collation : Collation?
    getter hint : (String | BSON)?

    def initialize(filter, @collation = nil, @hint = nil)
      @filter = BSON.new(filter)
    end
  end

  # Delete one or more documents.
  struct DeleteMany < WriteModel
    getter filter : BSON
    getter collation : Collation?
    getter hint : (String | BSON)?

    def initialize(filter, @collation = nil, @hint = nil)
      @filter = BSON.new(filter)
    end
  end

  # Replace one document.
  struct ReplaceOne < WriteModel
    getter filter : BSON
    getter replacement : BSON
    getter collation : Collation?
    getter hint : (String | BSON)?
    getter upsert : Bool?

    def initialize(filter, replacement, @collation = nil, @hint = nil, @upsert = nil)
      @filter = BSON.new(filter)
      @replacement = BSON.new(replacement)
    end
  end

  # Update one document.
  struct UpdateOne < WriteModel
    getter filter : BSON
    getter update : BSON | Array(BSON)
    getter array_filters : Array(BSON)?
    getter collation : Collation?
    getter hint : (String | BSON)?
    getter upsert : Bool?

    def initialize(filter, update, @array_filters = nil, @collation = nil, @hint = nil, @upsert = nil)
      @filter = BSON.new(filter)
      @update = update
    end
  end

  # Update one or more documents.
  struct UpdateMany < WriteModel
    getter filter : BSON
    getter update : BSON | Array(BSON)
    getter array_filters : Array(BSON)?
    getter collation : Collation?
    getter hint : (String | BSON)?
    getter upsert : Bool?

    def initialize(filter, update, @array_filters = nil, @collation = nil, @hint = nil, @upsert = nil)
      @filter = BSON.new(filter)
      @update = update
    end
  end

  # An aggregated result of the server replies.
  class WriteResult
    property n_inserted : Int32 = 0
    property n_matched : Int32 = 0
    property n_modified : Int32 = 0
    property n_removed : Int32 = 0
    property n_upserted : Int32 = 0
    property upserted : Array(Commands::Common::Upserted) = [] of Commands::Common::Upserted
    property write_errors : Array(Commands::Common::WriteError) = [] of Commands::Common::WriteError
    property write_concern_errors : Array(Commands::Common::WriteConcernError) = [] of Commands::Common::WriteConcernError
  end

  # Insert a single document.
  def insert_one(document)
    @models << InsertOne.new(BSON.new document)
    self
  end

  # Delete a single document.
  def delete_one(filter, **options)
    @models << DeleteOne.new(BSON.new(filter), **options)
    self
  end

  # Delete one or more documents.
  def delete_many(filter, **options)
    @models << DeleteMany.new(BSON.new(filter), **options)
    self
  end

  # Replace one document.
  def replace_one(filter, replacement, **options)
    @models << ReplaceOne.new(BSON.new(filter), BSON.new(replacement), **options)
    self
  end

  # Update one document.
  def update_one(filter, update, **options)
    @models << UpdateOne.new(BSON.new(filter), BSON.new(update), **options)
    self
  end

  # Update many documents.
  def update_many(filter, update, **options)
    @models << UpdateMany.new(BSON.new(filter), BSON.new(update), **options)
    self
  end

  # Execute the bulk operations stored in this `Bulk` instance.
  def execute(write_concern : WriteConcern? = nil, bypass_document_validation : Bool? = nil)
    _, not_executed = @executed.compare_and_set(0_u8, 1_u8)
    raise Mongo::Bulk::Error.new "Cannot execute a bulk operation more than once" unless not_executed

    options = {
      bypass_document_validation: bypass_document_validation,
      write_concern:              write_concern,
    }

    models = @models
    unless @ordered
      # Reorder based on the operation type
      models.sort!
    end
    # Group by operation type.
    group_type = nil
    group = [] of BSON
    group_bytesize = 0
    index_offset = 0
    results = WriteResult.new

    models.each { |model|
      if model.class != group_type
        index_offset = process_group(group_type, group, results, index_offset, options)
        return results if early_return?(results)
        group_type = model.class
        group_bytesize = 0
      end

      bson = format_bson(model)

      if group_bytesize + bson.size >= @max_bson_object_size || group.size >= @max_bson_object_size
        index_offset = process_group(group_type, group, results, index_offset, options)
        return results if early_return?(results)
        group_bytesize = 0
      end

      group << bson
      group_bytesize += bson.size
    }

    process_group(group_type, group, results, index_offset, options)

    results
  end

  private def format_bson(model : WriteModel) : BSON
    case model
    when InsertOne
      model.document
    when DeleteOne
      Tools.merge_bson({
        q:     model.filter,
        limit: 1,
      }, {
        hint:      model.hint,
        collation: model.collation,
      }) { |_, value|
        value.nil?
      }
    when DeleteMany
      Tools.merge_bson({
        q:     model.filter,
        limit: 0,
      }, {
        hint:      model.hint,
        collation: model.collation,
      }) { |_, value|
        value.nil?
      }
    when ReplaceOne
      Tools.merge_bson({
        q:     model.filter,
        u:     model.replacement,
        multi: false,
      }, {
        hint:      model.hint,
        collation: model.collation,
        upsert:    model.upsert,
      }) { |_, value|
        value.nil?
      }
    when UpdateOne
      Tools.merge_bson({
        q:     model.filter,
        u:     model.update,
        multi: false,
      }, {
        hint:          model.hint,
        collation:     model.collation,
        upsert:        model.upsert,
        array_filters: model.array_filters,
      }) { |_, value|
        value.nil?
      }
    when UpdateMany
      Tools.merge_bson({
        q:     model.filter,
        u:     model.update,
        multi: true,
      }, {
        hint:          model.hint,
        collation:     model.collation,
        upsert:        model.upsert,
        array_filters: model.array_filters,
      }) { |_, value|
        value.nil?
      }
    else
      raise Mongo::Bulk::Error.new "Invalid Operation"
    end.not_nil!
  end

  private def process_group(type, group : Array(BSON), results, index_offset, options) : Int32
    return 0 if group.size < 1

    options = options.merge({
      ordered: @ordered,
    })

    if type == InsertOne
      result = @collection.command(Commands::Insert, documents: group, options: options)
    elsif type == DeleteOne
      result = @collection.command(Commands::Delete, deletes: group, options: options)
    elsif type == DeleteMany
      result = @collection.command(Commands::Delete, deletes: group, options: options)
    elsif type == ReplaceOne
      result = @collection.command(Commands::Update, updates: group, options: options)
    elsif type == UpdateOne
      result = @collection.command(Commands::Update, updates: group, options: options)
    elsif type == UpdateMany
      result = @collection.command(Commands::Update, updates: group, options: options)
    else
      raise Mongo::Bulk::Error.new "Invalid Operation"
    end

    merge_results(results, result.not_nil!, index_offset)

    index_offset += group.size
    group.clear
    index_offset
  end

  private def merge_results(results, result, index_offset)
    case result
    when Commands::Common::InsertResult
      result.n.try { |n| results.n_inserted += n }
    when Commands::Common::DeleteResult
      result.n.try { |n| results.n_removed += n }
    when Commands::Common::UpdateResult
      upserted_size = result.upserted.try(&.size) || 0
      results.n_upserted += upserted_size
      result.n.try { |n|
        results.n_matched += (n - upserted_size)
      }
      result.n_modified.try { |n| results.n_modified += n }
      if upserted = result.upserted
        upserted.each { |upsert|
          results.upserted << Commands::Common::Upserted.new(
            upsert.index + index_offset,
            upsert._id
          )
        }
      end
    end

    if write_errors = result.write_errors
      write_errors.each { |write_error|
        results.write_errors << Commands::Common::WriteError.new(
          write_error.index + index_offset,
          write_error.code,
          write_error.errmsg
        )
      }
    end

    if write_concern_error = result.write_concern_error
      results.write_concern_errors << write_concern_error
    end
  end

  private def early_return?(results)
    @ordered && results.write_errors.size > 0
  end
end

# Is raised while trying to build or execute a bulk operation.
class Mongo::Bulk::Error < Mongo::Error
end
