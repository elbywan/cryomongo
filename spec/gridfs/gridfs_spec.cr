require "../spec_helper.cr"
require "./spec_helper.cr"

describe Mongo::GridFS do
  client = Mongo::Client.new
  database = client["gridfs_tests"]

  before_all {
    `rm -Rf ./data`
    start_mongo
  }

  after_all {
    client.close
    stop_mongo
    sleep 1
    `rm -Rf ./data`
  }

  context "features" {

    chunks_collection = database["gridfs_bucket.chunks"]
    chunk_size = 1000
    gridfs = database.grid_fs(bucket_name: "gridfs_bucket", chunk_size_bytes: chunk_size)

    file_name = "lorem.txt"
    file_path = "./spec/gridfs/#{file_name}"
    file = File.new(file_path)
    file_size = file.size
    file_contents = file.gets_to_end
    file.close

    before_each {
      gridfs.drop
    }

    it "should create IO streams to upload and download data", do
      id = "lorem_id"
      gridfs.open_upload_stream("lorem.txt", id: id) { |stream|
        # Fragment the contents into multiple short writes to ensure that it works across fibers.
        split = file_contents.split("\n")
        split.each_with_index { |fragment, index|
          stream << fragment
          stream << "\n" if index < split.size - 1
          Fiber.yield
        }
      }

      sleep 0.5

      gridfs_file = gridfs.find({ _id: id }).first
      gridfs_file.filename.should eq file_name
      gridfs_file.length.should eq file_size
      gridfs_file.chunk_size.should eq chunk_size
      gridfs_file.upload_date.should be < Time.utc
      gridfs_file.upload_date.should be > (Time.utc - 5.seconds)

      stream = gridfs.open_download_stream(id)
      stream.gets_to_end.should eq file_contents
    ensure
      stream.try &.close
    end

    it "should use provided IO streams to upload and download data" do
      id = "lorem_id"
      file = File.new(file_path)
      gridfs.upload_from_stream("lorem.txt", stream: file, id: id)

      sleep 0.5

      gridfs_file = gridfs.find({ _id: id }).first
      gridfs_file.filename.should eq file_name
      gridfs_file.length.should eq file_size
      gridfs_file.chunk_size.should eq chunk_size
      gridfs_file.upload_date.should be < Time.utc
      gridfs_file.upload_date.should be > (Time.utc - 5.seconds)

      stream = IO::Memory.new
      gridfs.download_to_stream(id, stream)
      stream.rewind.gets_to_end.should eq file_contents
    ensure
      file.try &.close
    end

    it "should download a file by name and revision" do
      10.times { |i|
        io = IO::Memory.new("#{i}")
        gridfs.upload_from_stream("file", stream: io, id: i)
      }

      10.times { |i|
        io = IO::Memory.new
        gridfs.download_to_stream_by_name("file", io, i)
        io.rewind.gets_to_end.should eq "#{i}"
        gridfs.download_to_stream_by_name("file", io.rewind, -i-1)
        io.rewind.gets_to_end.should eq "#{9 - i}"

        stream = gridfs.open_download_stream_by_name("file", i)
        stream.gets_to_end.should eq "#{i}"
        stream.close
        stream = gridfs.open_download_stream_by_name("file", -i-1)
        stream.gets_to_end.should eq "#{9 - i}"
        stream.close
      }
    end

    it "should find files" do
      10.times { |i|
        io = IO::Memory.new("#{i}")
        gridfs.upload_from_stream("file_#{i}", stream: io, id: i)
      }

      files = gridfs.find({
        _id: { "$gte": 5 }
      }).to_a
      files.size.should eq 5

      files.each_with_index { |f, idx|
        f.filename.should eq "file_#{idx + 5}"
        f._id.should eq (idx + 5)
      }
    end

    it "should delete a file" do
      10.times { |i|
        io = IO::Memory.new("#{i}")
        gridfs.upload_from_stream("file_#{i}", stream: io, id: i)
      }

      gridfs.find.to_a.size.should eq 10
      gridfs.find({ _id: 5 }).to_a.size.should eq 1
      gridfs.delete(5)
      gridfs.find.to_a.size.should eq 9
      gridfs.find({ _id: 5 }).to_a.size.should eq 0
    end

    it "should rename a file" do
      id = gridfs.upload_from_stream("file", stream: IO::Memory.new("12345678"))
      gridfs.rename(id, "file2")
      gridfs.find({ _id: id }).first.filename.should eq "file2"
    end

    it "should drop all files and chunks" do
      10.times { |i|
        io = IO::Memory.new("#{i}")
        gridfs.upload_from_stream("file_#{i}", stream: io, id: i)
      }

      gridfs.find.to_a.size.should eq 10
      chunks_collection.count_documents.should be > 0

      gridfs.drop

      gridfs.find.to_a.size.should eq 0
      chunks_collection.count_documents.should eq 0
    end
  }

  context "corpus" do
    gridfs = database.grid_fs

    Dir.glob "./spec/gridfs/tests/*.json" do |file_path|
      context file_path do
        test_file = JSON.parse(File.open(file_path) { |file|
          file.gets_to_end
        })

        tests = test_file["tests"].as_a
        data  = test_file["data"].as_h

        before_each {
          files = data["files"].as_a.map{|elt| BSON.from_json(elt.to_json)}
          chunks = data["chunks"].as_a.map{|elt| BSON.from_json(elt.to_json)}
          database["fs.files"].insert_many(files) if files.size > 0
          database["fs.chunks"].insert_many(chunks) if chunks.size > 0
          database["expected.files"].insert_many(files) if files.size > 0
          database["expected.chunks"].insert_many(chunks) if chunks.size > 0
        }

        after_each {
          database["expected.files"].delete_many(BSON.new)
          database["expected.chunks"].delete_many(BSON.new)
          gridfs.drop
        }

        tests.each do |test|
          description = test["description"].as_s
          arrange = test["arrange"]?.try &.as_h
          act = test["act"].as_h
          assert = test["assert"].as_h
          focus = test["focus"]?.try(&.as_bool) || false

          it description, focus: focus do
            run_data_commands(arrange, client, db_name: "gridfs_tests")
            expect_error = assert["error"]?

            if expect_error
              expect_raises(Exception) {
                act(act, gridfs)
              }
            else
              result = act(act, gridfs)
            end

            apply_expected_data(assert, client, db_name: "gridfs_tests", result: result)
            compare_with_expected(database)

            if expected_result = assert["result"]?
              if hex_result = (expected_result.as_h?.try &.["$hex"]?)
                result.should eq hex_result.as_s.hexbytes
              end
            end
          end
        end
      end
    end
  end
end
