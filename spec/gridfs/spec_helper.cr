struct BSON
  private module Decoder
    protected def decode_json_object(inner_key : String, kind : JSON::PullParser::Kind, key : String, builder : Builder, pull : JSON::PullParser)
      if inner_key == "$hex"
        bytes = pull.read_string.hexbytes
        builder[key] = Binary.new(:generic, bytes)
      else
        previous_def
      end
    end
  end
end

class Mongo::Client
  def run_command(body : BSON)
    # Dummy arguments
    server_description = server_selection(Commands::Insert, NamedTuple.new, read_preference: ReadPreference.new(mode: "primary"))
    connection = get_connection(server_description)
    op_msg = Messages::OpMsg.new(body)
    connection.send(op_msg)
    connection.receive
  end
end

def run_data_commands(bson, client, *, db_name)
  bson.try &.["data"]?.try &.as_a.each { |command|
    bson_command = BSON.from_json(command.to_json)
    bson_command["$db"] = db_name
    client.run_command(bson_command)
  }
end

private def transform_expected_data(bson, result)
  updated_bson = BSON.new
  bson.each { |key, value, code|
    if value == "*actual"
      value = "*actual #{Random::Secure.hex}"
    elsif value == "*result"
      value = result
    end

    if value.is_a? BSON
      if code.array?
        updated_bson.append_array(key, transform_expected_data(value, result))
      else
        updated_bson[key] = transform_expected_data(value, result)
      end
    else
      updated_bson[key] = value
    end
  }
  updated_bson
end

def apply_expected_data(json, client, *, db_name, result = nil, array = false)
  json.try &.["data"]?.try &.as_a.each { |json_command|
    command = BSON.from_json(json_command.to_json)
    updated_command = transform_expected_data(command, result)
    updated_command["$db"] = db_name
    client.run_command(updated_command)
  }
end

def act(act, gridfs)
  operation = act["operation"].as_s
  arguments = BSON.from_json(act["arguments"].to_json)
  case operation
  when "delete"
    id = arguments["id"].as(BSON::ObjectId)
    gridfs.delete(id)
  when "download"
    id = arguments["id"].as(BSON::ObjectId)
    stream = IO::Memory.new
    gridfs.download_to_stream(id, stream)
    stream.to_slice
  when "download_by_name"
    filename = arguments["filename"].as(String)
    revision = arguments.dig?(:options, :revision).try &.as(Int64).to_i32
    stream = IO::Memory.new
    gridfs.download_to_stream_by_name(filename, destination: stream, revision: revision || -1)
    stream.to_slice
  when "upload"
    filename = arguments["filename"].as(String)
    source = arguments["source"].as(Slice)
    binary_io = IO::Memory.new(source)
    chunk_size_bytes = arguments.dig?(:options, :chunkSizeBytes).try &.as(Int64).to_i32
    metadata = arguments.dig?(:options, :metadata).try &.as(BSON)
    gridfs.upload_from_stream(filename, stream: binary_io, chunk_size_bytes: chunk_size_bytes, metadata: metadata)
  else
    raise "Unknown operation: #{operation}"
  end
end

private def deep_compare(actual, expected)
  unless expected.is_a? BSON
    return actual.should eq expected
  end

  expected.each { |key, value|
    if value.is_a? BSON
      deep_compare(value, actual[key])
    else
      if key == "md5" || key == "contentType" || key == "aliases"
        # Ignore, md5 contentType and aliases keys are deprecated.
      else
        actual[key].should eq value unless value.is_a? String && value.starts_with? "*actual"
      end
    end
  }
end

def compare_with_expected(database)
  files = database["fs.files"].find.to_a
  expected_files = database["expected.files"].find.to_a

  files.zip(expected_files).each { |(actual, expected)|
    deep_compare(actual, expected)
  }

  chunks = database["chunks"].find.to_a
  expected_chunks = database["expected.chunks"].find.to_a

  chunks.zip(expected_chunks).each { |(actual, expected)|
    deep_compare(actual, expected)
  }
end
