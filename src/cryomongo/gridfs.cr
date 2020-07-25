require "./database"
require "./error"

# GridFS is a specification for storing and retrieving files that exceed the BSON-document size limit of 16 MB.
module Mongo::GridFS
  # A GridFS file document.
  @[BSON::Options(camelize: "lower")]
  struct File(FileID)
    include BSON::Serializable

    # A unique ID for this document. Usually this will be of type ObjectId, but a custom _id value provided by the application may be of any type.
    property _id : FileID
    # The name of this stored file; this does not need to be unique.
    property filename : String = ""
    # The length of this stored file, in bytes.
    property length : Int64
    # The size, in bytes, of each data chunk of this file. This value is configurable by file. The default is 255 KiB.
    property chunk_size : Int64
    # The date and time this file was added to GridFS, stored as a BSON datetime value.
    property upload_date : Time
    # Any additional application data the user wishes to store.
    property metadata : BSON?
  end

  # A GridFS chunk document.
  private struct Chunk(FileID)
    include BSON::Serializable

    # A unique ID for this document of type BSON ObjectId.
    property _id : BSON::ObjectId
    # The id for this file (the _id from the files collection document). This field takes the type of the corresponding _id in the files collection.
    property files_id : FileID
    # The index number of this chunk, zero-based.
    property n : Int32
    # A chunk of data from the user file.
    property data : Bytes
  end

  # A configured GridFS bucket instance.
  class Bucket
    @completed_indexes_check = false

    # Creates a new GridFSBucket object, managing a GridFS bucket within the given database.
    def initialize(
      @db : Database,
      *,
      # The bucket name. Defaults to 'fs'.
      @bucket_name : String = "fs",
      # The chunk size in bytes. Defaults to 255 KiB.
      @chunk_size_bytes : Int32 = 255 * 1024,
      @write_concern : WriteConcern? = nil,
      @read_concern : ReadConcern? = nil,
      @read_preference : ReadPreference? = nil
    )
    end

    private def write_concern
      @write_concern || @db.write_concern
    end

    private def read_concern
      @read_concern || @db.read_concern
    end

    private def read_preference
      @read_preference || @db.read_preference
    end

    # Opens an `IO` stream that the caller can write the contents of the file to.
    #
    # NOTE: It is the responsbility of the caller to flush and close the stream.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # io = gridfs.open_upload_stream(filename: "file.txt", chunk_size_bytes: 1024, metadata: {hello: "world"})
    # io << "some" << "text"
    # io.flush
    # io.close
    # sleep 1
    # ```
    def open_upload_stream(
      filename : String,
      *,
      id = nil,
      chunk_size_bytes : Int32? = nil,
      metadata = nil
    ) : IO
      id ||= BSON::ObjectId.new
      chunk_size : Int32 = chunk_size_bytes || @chunk_size_bytes

      check_indexes(bucket, chunks)

      reader, writer = IO.pipe
      reader.buffer_size = chunk_size
      writer.buffer_size = chunk_size

      spawn same_thread: true do
        index = 0
        length = 0_i64
        buffer = Bytes.new(chunk_size)
        loop do
          read_bytes = fill_slice(reader, buffer.to_slice)
          break if read_bytes == 0
          data = buffer.to_slice[0, read_bytes]
          chunks.insert_one({
            files_id: id,
            n:        index,
            data:     data,
          }, write_concern: write_concern)
          length += read_bytes
          index += 1_i64
          break if read_bytes < chunk_size
        rescue IO::EOFError
          break
        end

        bucket.insert_one({
          _id:        id,
          length:     length,
          chunkSize:  chunk_size,
          uploadDate: Time.utc,
          filename:   filename,
          metadata:   metadata,
        }, write_concern: write_concern)
      ensure
        reader.close
      end

      writer
    end

    # Yields an `IO` stream that the caller can write the contents of the file to.
    #
    # NOTE: Will flush and close the stream after the block gets executed.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # gridfs.open_upload_stream(filename: "file.txt", chunk_size_bytes: 1024, metadata: {hello: "world"}) { |io|
    #   io << "some text"
    # }
    # sleep 1
    # ```
    def open_upload_stream(
      filename : String,
      *,
      id : FileID = nil,
      chunk_size_bytes : Int32? = nil,
      metadata = nil,
      &block
    ) forall FileID
      id ||= BSON::ObjectId.new
      stream = open_upload_stream(filename, id: id, chunk_size_bytes: chunk_size_bytes, metadata: metadata)
      yield stream
      stream.flush
      Fiber.yield
      stream.close
      id
    end

    # Uploads a user file to a GridFS bucket.
    #
    # The application supplies a custom file id or the driver will generate the file id.
    #
    # Reads the contents of the user file from the *source* Stream and uploads it
    # as chunks in the chunks collection. After all the chunks have been uploaded,
    # it creates a files collection document for *filename* in the files collection.
    #
    # Returns the id of the uploaded file.
    #
    # NOTE: It is the responsbility of the caller to flush and close the stream.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # file = File.new("file.txt")
    # id = gridfs.upload_from_stream("file.txt", file)
    # file.close
    # puts id
    # ```
    def upload_from_stream(
      filename : String,
      stream : IO,
      *,
      id : FileID = nil,
      chunk_size_bytes : Int32? = nil,
      metadata = nil
    ) forall FileID
      id ||= BSON::ObjectId.new
      chunk_size_bytes ||= @chunk_size_bytes

      check_indexes(bucket, chunks)

      index = 0
      length = 0_i64
      buffer = Bytes.new(chunk_size_bytes)
      while (read_bytes = stream.read(buffer.to_slice)) > 0
        data = buffer.to_slice[0, read_bytes]
        chunks.insert_one({
          files_id: id,
          n:        index,
          data:     data,
        }, write_concern: write_concern)
        length += read_bytes
        index += 1_i64
      end

      bucket.insert_one({
        _id:        id,
        length:     length,
        chunkSize:  chunk_size_bytes,
        uploadDate: Time.utc,
        filename:   filename,
        metadata:   metadata,
      }, write_concern: write_concern)

      id
    end

    # Opens a Stream from which the application can read the contents of the stored file
    # specified by *id*.
    #
    # Returns a `IO` stream.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # id = BSON::ObjectId.new("5eed35600000000000000000")
    # stream = gridfs.open_download_stream(id)
    # puts stream.gets_to_end
    # stream.close
    # ```
    def open_download_stream(id : FileID) : IO forall FileID
      file = get_file(id)
      count = chunk_count(file)
      remaining = file.length

      reader, writer = IO.pipe
      reader.buffer_size = file.chunk_size.to_i32
      writer.buffer_size = file.chunk_size.to_i32

      spawn same_thread: true do
        count.times { |n|
          chunk = get_chunk(id, n)
          integrity_check!(file, chunk, remaining)
          writer.write(chunk.data)
          remaining -= chunk.data.size
        }
      ensure
        writer.close
      end

      reader
    end

    # Downloads the contents of the stored file specified by *id* and writes
    # the contents to the *destination* Stream.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # stream = IO::Memory.new
    # id = BSON::ObjectId.new("5eed35600000000000000000")
    # gridfs.download_to_stream(id, stream)
    # puts stream.rewind.gets_to_end
    # ```
    def download_to_stream(id : FileID, destination : IO) : Nil forall FileID
      file = get_file(id)
      count = chunk_count(file)
      remaining = file.length

      count.times { |n|
        chunk = get_chunk(id, n)
        integrity_check!(file, chunk, remaining)
        destination.write(chunk.data)
        remaining -= chunk.data.size
      }
    end

    # Opens a `IO` stream from which the application can read the contents of the stored file
    # specified by *filename* and an optional *revision*.
    #
    # Returns a `IO` stream.
    #
    # NOTE: It is the responsbility of the caller to close the stream.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # stream = gridfs.open_download_stream_by_name("file", revision: 2)
    # puts stream.gets_to_end
    # stream.close
    # ```
    #
    # #### About the the *revision* argument:
    #
    # Specifies which revision (documents with the same filename and different uploadDate)
    # of the file to retrieve. Defaults to -1 (the most recent revision).
    #
    # Revision numbers are defined as follows:
    # - 0 = the original stored file
    # - 1 = the first revision
    # - 2 = the second revision
    #
    # etc…
    #
    # - -2 = the second most recent revision
    # - -1 = the most recent revision
    def open_download_stream_by_name(filename : String, revision : Int32 = -1) : IO
      file = get_file_by_name(filename, revision)
      count = chunk_count(file)
      reader, writer = IO.pipe
      reader.buffer_size = file.chunk_size.to_i32
      writer.buffer_size = file.chunk_size.to_i32

      spawn same_thread: true do
        remaining = file.length
        count.times { |n|
          chunk = get_chunk(file._id, n)
          integrity_check!(file, chunk, remaining)
          writer.write(chunk.data)
          remaining -= chunk.data.size
        }
      ensure
        writer.close
      end

      reader
    end

    # Downloads the contents of the stored file specified by *filename* and by an optional *revision* and writes the contents to the *destination* `IO` stream.
    #
    # See: `open_download_stream_by_name` for how the revision is calculated.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # io = IO::Memory.new
    # gridfs.download_to_stream_by_name("file", io, revision: -1)
    # puts io.to_s
    # ```
    def download_to_stream_by_name(filename : String, destination : IO, revision : Int32 = -1) : Nil
      file = get_file_by_name(filename, revision)
      count = chunk_count(file)
      remaining = file.length

      count.times { |n|
        chunk = get_chunk(file._id, n)
        integrity_check!(file, chunk, remaining)
        destination.write(chunk.data)
        remaining -= chunk.data.size
      }
    end

    # Given an *id*, delete this stored file’s files collection document and associated chunks from a GridFS bucket.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # id = BSON::ObjectId.new("5eed35600000000000000000")
    # gridfs.delete(id)
    # ```
    def delete(id : FileID) : Nil forall FileID
      delete_result = bucket.delete_one({_id: id}, write_concern: write_concern)
      chunks.delete_many({files_id: id}, write_concern: write_concern)
      raise Mongo::Error.new "File not found." if delete_result.try &.n == 0
    end

    # Find and return the files collection documents that match *filter*.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # gridfs.find({
    #   length: {"$gte": 5000},
    # })
    # ```
    def find(
      filter = BSON.new,
      *,
      allow_disk_use : Bool? = nil,
      batch_size : Int32? = nil,
      limit : Int32? = nil,
      max_time_ms : Int64? = nil,
      no_cursor_timeout : Bool? = nil,
      skip : Int32? = nil,
      sort = nil
    ) : Cursor::Wrapper(File(BSON::Value))
      cursor = bucket.find(
        filter,
        allow_disk_use: allow_disk_use,
        batch_size: batch_size,
        limit: batch_size,
        max_time_ms: batch_size,
        no_cursor_timeout: batch_size,
        skip: batch_size,
        sort: sort,
        read_concern: read_concern,
        read_preference: read_preference
      )
      Cursor::Wrapper(File(BSON::Value)).new(cursor)
    end

    # Renames the stored file with the specified *id*.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # id = BSON::ObjectId.new("5eed35600000000000000000")
    # gridfs.rename(id, new_filename: "new_name.txt")
    # ```
    def rename(id : FileID, new_filename : String) : Nil forall FileID
      bucket.update_one({_id: id}, {"$set": {filename: new_filename}})
    end

    # Drops the files and chunks collections associated with this bucket.
    #
    # ```
    # gridfs = client["database"].grid_fs
    # gridfs.drop
    # ```
    def drop
      bucket.delete_many(BSON.new)
      chunks.delete_many(BSON.new)
    end

    private module Internal
      def bucket
        @db["#{@bucket_name}.files"]
      end

      def chunks
        @db["#{@bucket_name}.chunks"]
      end

      def check_indexes(bucket, chunks)
        # see: https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst#before-write-operations
        return if @completed_indexes_check
        check_collection_index(bucket, {filename: 1, uploadDate: 1})
        check_collection_index(chunks, {files_id: 1, n: 1})
        @completed_indexes_check = true
      end

      def check_collection_index(collection, keys)
        return if collection.find_one(projection: {_id: 1})

        begin
          indexes = collection.list_indexes.to_a
        rescue e
          # Collection might not exist and listing indexes will raise.
        end
        return if indexes.try &.any? { |index|
                    index["key"]?.try &.as(BSON).all? { |key, value|
                      keys[key]?.try &.== value
                    }
                  }

        collection.create_index(
          keys: keys
        )
      end

      def fill_slice(io : IO, slice : Bytes)
        count = 0
        while slice.size > 0
          read_bytes = io.read slice
          break if read_bytes == 0
          count += read_bytes
          slice += read_bytes
        end
        count
      end

      def get_file(id : FileID) : File(FileID) forall FileID
        file = bucket.find_one({_id: id}, read_preference: read_preference, read_concern: read_concern)
        raise Mongo::Error.new "Cannot find file with id: #{id}" unless file
        File(FileID).from_bson(file)
      end

      def get_file_by_name(name : String, revision : Int32 = -1) : File(BSON::Value)
        sort_order = revision >= 0 ? 1 : -1
        file = bucket.find_one(
          {filename: name},
          sort: {uploadDate: sort_order},
          skip: revision >= 0 ? revision : -revision - 1,
          read_preference: read_preference,
          read_concern: read_concern
        )
        raise Mongo::Error.new "Cannot find revision #{revision} of the file named: #{name}" unless file
        File(BSON::Value).from_bson(file)
      end

      def chunk_count(file : File(FileID)) : Int64 forall FileID
        (file.length / file.chunk_size).ceil.to_i64
      end

      def get_chunk(id : FileID, n : Int64) forall FileID
        chunk = chunks.find_one({files_id: id, n: n}, sort: { n: 1 }, read_preference: read_preference, read_concern: read_concern)
        raise Mongo::Error.new "Chunk not found" unless chunk
        Chunk(FileID).from_bson(chunk)
      end

      def integrity_check!(file : File, chunk : Chunk, remaining : Int64)
        if chunk.data.size != remaining && chunk.data.size < file.chunk_size
          raise Mongo::Error.new "Wrong chunk size"
        end
      end
    end

    include Internal
  end
end
