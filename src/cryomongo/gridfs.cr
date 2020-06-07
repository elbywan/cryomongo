module Mongo::GridFS
  class Bucket
    def initialize(
      @db : Database,
      *,
      # The bucket name. Defaults to 'fs'.
      @bucket_name : String = "fs",
      # The chunk size in bytes. Defaults to 255 KiB.
      @chunk_size_bytes : Int32 = 255 * 1024,
      write_concern : WriteConcern? = nil,
      read_concern : ReadConcern? = nil,
      read_preference : ReadPreference? = nil
    )
      @write_concern    = write_concern   || db.write_concern
      @read_concern     = read_concern    || db.read_concern
      @read_preference  = read_preference || db.read_preference
    end

    private def check_indexes
      # TODO
    end


    # Opens a Stream that the application can write the contents of the file to.
    # The driver generates the file id.
    #
    # Returns a Stream to which the application will write the contents.
    #
    # Note: this method is provided for backward compatibility. In languages
    # that use generic type parameters, this method may be omitted since
    # the TFileId type might not be an ObjectId.
    # Stream open_upload_stream(string filename, GridFSUploadOptions options=null);

    # Opens a Stream that the application can write the contents of the file to.
    # The application provides a custom file id.
    #
    # Returns a Stream to which the application will write the contents.
    def open_upload_stream(
      filename : String,
      *,
      id = nil,
      chunk_size_bytes : Int32? = nil,
      metadata = nil
    ) : IO
      id ||= BSON::ObjectId.new
      chunk_size_bytes ||= @chunk_size_bytes
      bucket = @db[@bucket_name]
      chunks = @db["chunks"]

      check_indexes

      reader, writer = IO.pipe

      spawn do
        index = 0
        length = 0_i64
        buffer = Bytes.new(chunk_size_bytes)
        loop do
          read_bytes = reader.read(buffer.to_slice)
          break if read_bytes < chunk_size_bytes
          data = buffer.to_slice[0, read_bytes]
          chunks.insert_one({
            files_id: id,
            n: index,
            data: data,
          }, write_concern: @write_concern)
          length += read_bytes
          index += 1_i64
        rescue IO::EOFError
          break
        end

        bucket.insert_one({
          _id: id,
          length: length,
          chunkSize: chunk_size_bytes,
          uploadDate: Time.utc,
          filename: filename,
          metadata: metadata
        }, write_concern: @write_concern)
      end

      writer
    end

    # :ditto:
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
      Fiber.yield
      stream.close
      id
    end

    # Uploads a user file to a GridFS bucket. The driver generates the file id.
    #
    # Reads the contents of the user file from the @source Stream and uploads it
    # as chunks in the chunks collection. After all the chunks have been uploaded,
    # it creates a files collection document for @filename in the files collection.
    #
    # Returns the id of the uploaded file.
    #
    # Note: this method is provided for backward compatibility. In languages
    # that use generic type parameters, this method may be omitted since
    # the TFileId type might not be an ObjectId.
    # ObjectId upload_from_stream(string filename, Stream source, GridFSUploadOptions options=null);
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
      bucket = @db[@bucket_name]
      chunks = @db["chunks"]

      check_indexes

      index = 0
      length = 0_i64
      buffer = Bytes.new(chunk_size_bytes)
      while (read_bytes = stream.read(buffer.to_slice)) > 0
        data = buffer.to_slice[0, read_bytes]
        chunks.insert_one({
          files_id: id,
          n: index,
          data: data
        }, write_concern: @write_concern)
        length += read_bytes
        index += 1_i64
      end

      bucket.insert_one({
        _id: id,
        length: length,
        chunkSize: chunk_size_bytes,
        uploadDate: Time.utc,
        filename: filename,
        metadata: metadata
      }, write_concern: @write_concern)

      id
    end

    # Uploads a user file to a GridFS bucket. The application supplies a custom file id.
    #
    # Reads the contents of the user file from the @source Stream and uploads it
    # as chunks in the chunks collection. After all the chunks have been uploaded,
    # it creates a files collection document for @filename in the files collection.
    #
    # Note: there is no need to return the id of the uploaded file because the application
    # already supplied it as a a parameter.
    # void upload_from_stream_with_id(TFileId id, string filename, Stream source, GridFSUploadOptions options=null);
  end
end
