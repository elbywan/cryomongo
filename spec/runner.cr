module Mongo::Spec::Runner
  extend self

  # Runs a battery of tests conform to the transactions test suite format.
  # See: https://github.com/mongodb/specifications/blob/master/source/transactions/tests/README.rst#test-format
  def run_tests(path, get_client, topology, *, uri = "mongodb://localhost:27017/", ignore_cursor_42s = false, distinct_workaround = false)
    Dir.glob(path) do |file_path|
      test_file = JSON.parse(File.open(file_path) { |file|
        file.gets_to_end
      })

      skip_test = if run_on = test_file["runOn"]?.try(&.as_a)
                    run_on.all? { |constraint|
                      check_constraints(constraint, topology)
                    }
                  else
                    check_constraints(test_file, topology)
                  end

      if skip_test
        # puts "Skipping #{file_path} for topology #{topology} and version #{SERVER_VERSION}."
        next
      end

      context "#{file_path}" do
        run(test_file, file_path, get_client, topology, uri, ignore_cursor_42s, distinct_workaround)
      end
    end
  end

  private def check_constraints(version_root, topology)
    min_server_version = version_root["minServerVersion"]?.try { |v| semantic(v.as_s) } || SERVER_VERSION
    max_server_version = version_root["maxServerVersion"]?.try { |v| semantic(v.as_s) } || SERVER_VERSION
    topologies = version_root["topology"]?.try &.as_a

    skip = max_server_version < SERVER_VERSION ||
           min_server_version > SERVER_VERSION ||
           (topologies && topologies.none? { |t|
             topology.to_s.underscore.starts_with?(t.as_s)
           })

    skip
  end

  def run(test_file, file_path, get_client, topology, uri, ignore_cursor_42s, distinct_workaround)
    tests = test_file["tests"].as_a
    bucket_name = test_file["bucket_name"]?.try &.as_s
    collection_name = test_file["collection_name"]?.try(&.as_s) || "collection"
    database_name = test_file["database_name"]?.try(&.as_s) || "database"

    tests.each { |test|
      next if test["ignore"]?

      client_options = test["clientOptions"]?.try &.as_h

      if client_options
        client_uri = "#{uri}?#{query_options(client_options)}"
      else
        client_uri = uri
      end

      description = test["description"].as_s
      focus = test["focus"]?.try(&.as_bool) || false
      fail_point = test["failPoint"]?.try &.as_h
      fail_points = [] of Hash(String, JSON::Any)
      skip_reason = test["skipReason"]?.try &.as_s
      outcome = test["outcome"]?.try &.as_h
      expectations = test["expectations"]?.try &.as_a
      session_options = test["sessionOptions"]?.try &.as_h
      use_multiple_mongoses = test["useMultipleMongoses"]?.try &.as_bool

      next if skip_reason || (use_multiple_mongoses ? topology.sharded? : topology.sharded_multiple_mongos?)

      it "#{description} (#{file_path})", focus: focus do
        client = get_client.call
        local_client = Mongo::Client.new(client_uri)
        global_database = client[database_name]
        local_database = local_client[database_name]
        majority_write_concern = Mongo::WriteConcern.new(w: "majority")

        begin
          client.command(Mongo::Commands::KillAllSessions, users: [] of String)
        rescue
          # Ignore (https://jira.mongodb.org/browse/SERVER-38335)
        end

        begin
          if bucket_name
            global_database.command(Mongo::Commands::Drop, name: "#{bucket_name}.files", write_concern: majority_write_concern)
            global_database.command(Mongo::Commands::Drop, name: "#{bucket_name}.chunks", write_concern: majority_write_concern)
            global_database.command(Mongo::Commands::Create, name: "#{bucket_name}.files", write_concern: majority_write_concern)
            global_database.command(Mongo::Commands::Create, name: "#{bucket_name}.chunks", write_concern: majority_write_concern)
          else
            begin
              global_database.command(Mongo::Commands::Drop, name: collection_name, write_concern: majority_write_concern)
            rescue
            end
            global_database.command(Mongo::Commands::Create, name: collection_name, write_concern: majority_write_concern)
          end
        rescue
          # Ignore because collection namespace might not exist
        end

        # https://github.com/mongodb/specifications/tree/master/source/transactions/tests#why-do-tests-that-run-distinct-sometimes-fail-with-staledbversion
        if distinct_workaround && (topology.sharded_multiple_mongos? || topology.sharded?)
          distinct_client = Mongo::Client.new(uri)
          begin
            distinct_client.command(Mongo::Commands::Distinct, database: database_name, collection: collection_name, key: "1")
          rescue
          end
          distinct_client.topology.servers.each do |server|
            if server.type.mongos?
              distinct_client.command(Mongo::Commands::Distinct, database: database_name, collection: collection_name, key: "1")
            end
          rescue e
            e
          ensure
            distinct_client.close
          end
        end

        if bn = bucket_name
          gridfs = local_database.grid_fs(bucket_name: bn) # , chunk_size_bytes: chunk_size)
          if (d = test_file["data"].as_h)
            files = d["fs.files"].as_a.map { |elt| BSON.from_json(elt.to_json) }
            chunks = d["fs.chunks"].as_a.map { |elt| BSON.from_json(elt.to_json) }
            global_database["#{bn}.files"].insert_many(files) if files.size > 0
            global_database["#{bn}.chunks"].insert_many(chunks) if chunks.size > 0
          end
        else
          data = test_file["data"]?.try &.as_a.map { |elt| BSON.from_json(elt.to_json) }
          if (d = data) && d.size > 0
            global_database[collection_name].insert_many(
              d,
              write_concern: majority_write_concern
            )
          end
        end

        if fp = fail_point
          fail_points << fp
          fp_mode = fp["mode"].as_s? || BSON.from_json fp["mode"].to_json
          fp_data = fp["data"]?.try { |fp_data_json| BSON.from_json(fp_data_json.to_json) }
          client.command(Mongo::Commands::ConfigureFailPoint, fail_point: fp["configureFailPoint"].as_s, mode: fp_mode, options: {data: fp_data})
        end

        session0, session1 = ["session0", "session1"].map do |session_nb|
          if session_options && (opts = session_options[session_nb]?.try(&.as_h?))
            causal_consistency = opts["causalConsistency"]?.try(&.as_bool)
            causal_consistency = causal_consistency.nil? ? true : causal_consistency
            if default_transaction_options = opts["defaultTransactionOptions"]?.try(&.as_h?)
              local_client.start_session(
                causal_consistency: causal_consistency,
                default_transaction_options: Mongo::Session::TransactionOptions.new(
                  read_concern: default_transaction_options["readConcern"]?.try { |read_concern|
                    Mongo::ReadConcern.from_bson(BSON.from_json(read_concern.to_json))
                  },
                  write_concern: default_transaction_options["writeConcern"]?.try { |write_concern|
                    Mongo::WriteConcern.from_bson(BSON.from_json(write_concern.to_json))
                  },
                  read_preference: default_transaction_options["readPreference"]?.try { |read_preference|
                    Mongo::ReadPreference.from_bson(BSON.from_json(read_preference.to_json))
                  },
                  max_commit_time_ms: default_transaction_options["maxCommitTimeMS"]?.try &.as_i64?
                ))
            else
              local_client.start_session(causal_consistency: causal_consistency)
            end
          else
            local_client.start_session
          end
        end

        sessions = {session0, session1}

        counter_ref = Pointer(Int32).malloc(1, 0)
        lsid_list = [] of BSON
        subscription = validate_expectations(expectations, counter_ref, local_client, lsid_list: lsid_list, sessions: sessions, command_started_only: true, ignore_cursor_42s: ignore_cursor_42s)

        operation = test["operation"]?.try &.as_h
        operations = operation.try { |o| [o] } || test["operations"].as_a.map(&.as_h)
        operations.each { |o|
          expect_error = o["error"]?.try &.as_bool || outcome.try &.["error"]?.try &.as_bool
          result = o["result"]? || outcome.try(&.["result"]?)
          collection = local_client[database_name][collection_name]
          error_contains = nil
          error_code_name = nil
          error_labels_contain = nil
          error_labels_omit = nil
          result.try &.as_h?.try { |res|
            error_contains = res["errorContains"]?.try &.as_s?
            error_code_name = res["errorCodeName"]?.try &.as_s?
            error_labels_contain = res["errorLabelsContain"]?.try &.as_a?
            error_labels_omit = res["errorLabelsOmit"]?.try &.as_a?
          }
          expect_error ||= error_contains || error_code_name || error_labels_contain || error_labels_omit

          if collection_options = o["collectionOptions"]?
            if read_concern = collection_options["readConcern"]?
              collection.read_concern = Mongo::ReadConcern.from_bson(BSON.from_json(read_concern.to_json))
            end
            if write_concern = collection_options["writeConcern"]?
              collection.write_concern = Mongo::WriteConcern.from_bson(BSON.from_json(write_concern.to_json))
            end
          end

          if database_options = o["databaseOptions"]?
            if read_concern = database_options["readConcern"]?
              local_database.read_concern = Mongo::ReadConcern.from_bson(BSON.from_json(read_concern.to_json))
            end
            if write_concern = database_options["writeConcern"]?
              local_database.write_concern = Mongo::WriteConcern.from_bson(BSON.from_json(write_concern.to_json))
            end
          end

          begin
            if expect_error
              expect_raises(Mongo::Error) do
                spec_operation(local_client, local_database, collection, o, outcome_result: result, sessions: sessions, lsid_list: lsid_list, gridfs: gridfs, fail_points: fail_points)
              rescue e : Mongo::Error
                if error_contains
                  e.message.should_not be_nil
                  e.message.try &.downcase.should contain error_contains.downcase
                end

                if error_code_name
                  e.should be_a(Mongo::Error::Command)
                  e.as(Mongo::Error::Command).code_name.should eq error_code_name
                end

                if error_labels_contain
                  error_labels_contain.each { |label|
                    e.error_labels.should contain label
                  }
                end

                if error_labels_omit
                  error_labels_omit.each { |label|
                    e.error_labels.should_not contain label
                  }
                end
                raise e
              end
            else
              spec_operation(local_client, local_database, collection, o, outcome_result: result, sessions: sessions, lsid_list: lsid_list, gridfs: gridfs)
            end
          rescue error : Mongo::Error::Network
            # caused by the fail points
          end
        }

        session0.end
        session1.end

        if e = expectations
          counter_ref.value.should eq e.size
        end

        validate_outcome(outcome, global_database, global_database[collection_name])
      ensure
        local_client.try { |c|
          subscription.try { |s| c.unsubscribe_commands(s) }
          c.close
        }
        if (c = client)
          fail_points.each { |fp|
            c.command(Mongo::Commands::ConfigureFailPoint, fail_point: fp["configureFailPoint"].as_s, mode: "off")
          }
        end
      end
    }
  end

  private macro bson_arg(name)
    arguments["{{name.id}}"]?.try { |%arg|
      BSON.from_json(%arg.to_json)
    }
  end

  private macro bson_array_arg(name)
    arguments["{{name.id}}"]?.try { |%arg|
      %arg.as_a.map{ |elt|
        BSON.from_json(elt.to_json)
      }
    }
  end

  private macro bson_or_array_arg(name)
    arguments["{{name.id}}"]?.try { |%arg|
      if %arg.as_a?
        bson_array_arg({{ name }})
      else
        bson_arg({{ name }})
      end
    }
  end

  private macro int32_arg(name)
    arguments["{{name.id}}"]?.try { |i|
      i.as_i? || i.as_h["$numberLong"].as_s.to_i
    }
  end

  private macro int64_arg(name)
    arguments["{{name.id}}"]?.try { |i|
      i.as_i64? || i.as_h["$numberLong"].as_s.to_i64
    }
  end

  private macro string_arg(name)
    arguments["{{name.id}}"]?.try &.as_s
  end

  private macro bool_arg(name, default = nil)
    if arguments["{{name.id}}"]?
      arguments["{{name.id}}"].as_bool
    else
      {{default}}
    end
  end

  def compare_json(a : JSON::Any, b : JSON::Any)
    compare_json(a, b) { |one, two|
      one.should eq two
    }
  end

  def compare_json(a : JSON::Any, b : JSON::Any, *, key : String? = nil, &block : (JSON::Any, JSON::Any, String?) ->)
    if a.as_a?
      a.as_a.each_with_index { |elt, index|
        compare_json(elt, b[index], key: key, &block)
      }
    elsif a.as_h?
      if (a_nb = a["$numberLong"]?) && b.as_i64?
        yield JSON::Any.new(a_nb.as_s.to_i64), b, key
      else
        a.as_h.each { |k, v|
          if v != nil
            compare_json(v, b.as_h[k], key: k, &block)
          elsif v == nil
            b.as_h[k]?.should be_nil
          end
        }
      end
    else
      yield a, b, key
    end
  end

  private def query_options(options)
    options.reduce([] of String) { |acc, (k, v)|
      acc << "#{k}=#{v}"
    }.join("&")
  end

  # Test an operation.
  def spec_operation(client, db, collection, operation, *, outcome_result = nil, sessions = nil, lsid_list = [] of BSON, gridfs = nil, fail_points = [] of Hash(String, JSON::Any))
    operation_name = operation["name"].as_s
    operation_object = operation["object"]?.try &.as_s || "collection"

    # Special operations

    if operation_object == "testRunner"
      case operation_name
      when "assertDifferentLsidOnLastTwoCommands"
        lsid_list.size.should be >= 2
        lsid_list[-1].should_not eq lsid_list[-2]
      when "assertSameLsidOnLastTwoCommands"
        lsid_list.size.should be >= 2
        lsid_list[-1].should eq lsid_list[-2]
      when "assertSessionDirty"
        session_name = operation.dig("arguments", "session").as_s
        session = if session_name == "session0"
                    sessions.not_nil![0]
                  elsif session_name == "session1"
                    sessions.not_nil![1]
                  end
        session.try &.dirty.should be_true
      when "assertSessionNotDirty"
        session_name = operation.dig("arguments", "session").as_s
        session = if session_name == "session0"
                    sessions.not_nil![0]
                  elsif session_name == "session1"
                    sessions.not_nil![1]
                  end
        session.try &.dirty.should be_false
      when "targetedFailPoint"
        session_name = operation.dig("arguments", "session").as_s
        fp = operation["arguments"].as_h["failPoint"].as_h
        fp_name = fp["configureFailPoint"].as_s
        fp_mode = fp["mode"].as_s? || BSON.from_json fp["mode"].to_json
        fp_data = fp["data"]?.try { |fp_data_json| BSON.from_json(fp_data_json.to_json) }
        session = if session_name == "session0"
                    sessions.not_nil![0]
                  elsif session_name == "session1"
                    sessions.not_nil![1]
                  end
        client.command(Mongo::Commands::ConfigureFailPoint, fail_point: fp_name, mode: fp_mode, options: {data: fp_data}, server_description: session.not_nil!.server_description.not_nil!)
        fail_points << fp
      when "assertSessionTransactionState"
        session_name = operation.dig("arguments", "session").as_s
        transition_label = operation.dig("arguments", "state").as_s
        session = if session_name == "session0"
                    sessions.not_nil![0]
                  elsif session_name == "session1"
                    sessions.not_nil![1]
                  end
        session.not_nil!.transaction_state.to_s.underscore.should eq transition_label
      when "assertSessionPinned"
        session_name = operation.dig("arguments", "session").as_s
        session = if session_name == "session0"
                    sessions.not_nil![0]
                  elsif session_name == "session1"
                    sessions.not_nil![1]
                  end
        session.not_nil!.server_description.should_not be_nil
      when "assertSessionUnpinned"
        session_name = operation.dig("arguments", "session").as_s
        session = if session_name == "session0"
                    sessions.not_nil![0]
                  elsif session_name == "session1"
                    sessions.not_nil![1]
                  end
        session.not_nil!.server_description.should be_nil
      when "assertCollectionExists"
        database_name = operation.dig("arguments", "database").as_s
        collection_name = operation.dig("arguments", "collection").as_s
        cursor = client[database_name].list_collections(filter: {name: collection_name})
        cursor.to_a.size.should eq 1
      when "assertCollectionNotExists"
        database_name = operation.dig("arguments", "database").as_s
        collection_name = operation.dig("arguments", "collection").as_s
        cursor = client[database_name].list_collections(filter: {name: collection_name})
        cursor.to_a.size.should eq 0
      when "assertIndexExists"
        database_name = operation.dig("arguments", "database").as_s
        collection_name = operation.dig("arguments", "collection").as_s
        index_name = operation.dig("arguments", "index").as_s
        cursor = client[database_name][collection_name].list_indexes
        cursor.to_a.any? { |index| index["name"] == index_name }.should be_true
      when "assertIndexNotExists"
        database_name = operation.dig("arguments", "database").as_s
        collection_name = operation.dig("arguments", "collection").as_s
        index_name = operation.dig("arguments", "index").as_s
        cursor = client[database_name][collection_name].list_indexes
        cursor.to_a.any? { |index| index["name"] == index_name }.should be_false
      end

      return
    end

    # Operations on sessions
    if operation_object == "session0" || operation_object == "session1"
      target_session = sessions.not_nil![operation_object == "session0" ? 0 : 1]

      case operation_name
      when "endSession"
        target_session.end
      when "startTransaction"
        options = operation.dig?("arguments", "options").try &.as_h?
        target_session.start_transaction(
          read_concern: options.try(&.["readConcern"]?).try { |rc| Mongo::ReadConcern.from_bson(BSON.from_json(rc.to_json)) },
          write_concern: options.try(&.["writeConcern"]?).try { |wc| Mongo::WriteConcern.from_bson(BSON.from_json(wc.to_json)) },
          read_preference: options.try(&.["readPreference"]?).try { |rp| Mongo::ReadPreference.from_bson(BSON.from_json(rp.to_json)) },
          max_commit_time_ms: options.try(&.["maxCommitTimeMS"]?.try &.as_i64?)
        )
      when "commitTransaction"
        target_session.commit_transaction
      when "abortTransaction"
        target_session.abort_transaction
      end

      return
    end

    # Arguments
    arguments = operation["arguments"]?.try(&.as_h) || ({} of String => JSON::Any)
    arguments["options"]?.try { |o|
      arguments = arguments.merge(o.as_h)
    }

    # Gridfs operations
    if operation_object == "gridfsbucket"
      gridfs = gridfs.not_nil!
      case operation_name
      when "delete"
        id = BSON::ObjectId.new(arguments["id"].as_h["$oid"].as_s)
        gridfs.delete(id)
      when "download"
        id = BSON::ObjectId.new(arguments["id"].as_h["$oid"].as_s)
        stream = IO::Memory.new
        gridfs.download_to_stream(id, stream)
      when "download_by_name"
        filename = arguments["filename"].as_s
        stream = IO::Memory.new
        gridfs.download_to_stream_by_name(filename, destination: stream)
      else
        raise "Unknown GridFS operation: #{operation}"
      end

      return
    end

    # Arguments parsing
    collation = arguments["collation"]?.try { |c|
      Mongo::Collation.from_bson(BSON.from_json(c.to_json))
    }
    filter = bson_arg "filter"
    update = bson_or_array_arg "update"
    replacement = bson_arg "replacement"
    document = bson_arg "document"
    documents = bson_array_arg "documents"
    upsert = bool_arg "upsert", default: false
    sort = bson_arg "sort"
    projection = bson_arg "projection"
    hint = arguments["hint"]?.try { |h|
      next h.as_s if h.as_s?
      BSON.from_json(h.to_json)
    }
    pipeline = bson_array_arg "pipeline"
    array_filters = bson_array_arg "arrayFilters"
    skip = int32_arg "skip"
    limit = int32_arg "limit"
    batch_size = int32_arg "batchSize"
    single_batch = bool_arg "singleBatch"
    max_time_ms = int64_arg "maxTimeMS"
    read_concern = arguments["readConcern"]?.try { |r|
      Mongo::ReadConcern.from_bson(BSON.from_json(r.to_json))
    }
    write_concern = arguments["writeConcern"]?.try { |w|
      Mongo::WriteConcern.from_bson(BSON.from_json(w.to_json))
    }
    allow_disk_use = bool_arg "allowDiskUse"
    bypass_document_validation = bool_arg "bypassDocumentValidation"
    ordered = bool_arg "ordered", default: true
    new_ = string_arg("returnDocument").try(&.== "After")
    fields = bson_arg "projection"

    if session_name = string_arg("session")
      if session_name == "session0"
        session = sessions.try &.[0]
      elsif session_name == "session1"
        session = sessions.try &.[1]
      end
    end

    # Transaction tests use this shape to trigger client errors apparently.
    if document.try &.["_id"]?.try &.as?(BSON).try &.has_key?(".")
      raise Mongo::Error::Client.new
    end

    # Database operations

    result = case operation_name
             when "estimatedDocumentCount"
               collection.estimated_document_count(
                 max_time_ms: max_time_ms.try &.to_i64,
                 session: session
               )
             when "countDocuments"
               collection.count_documents(
                 filter: filter,
                 skip: skip,
                 limit: limit,
                 collation: collation,
                 hint: hint,
                 max_time_ms: max_time_ms,
                 session: session
               )
             when "count"
               collection.command(Mongo::Commands::Count, session: session, options: {
                 query:     filter,
                 skip:      skip,
                 limit:     limit,
                 collation: collation,
               }).not_nil!.["n"].as(Int32)
             when "distinct"
               collection.distinct(
                 key: arguments["fieldName"].as_s,
                 filter: filter,
                 collation: collation,
                 session: session
               )
             when "find"
               collection.find(
                 filter: filter || BSON.new,
                 sort: sort,
                 projection: projection,
                 hint: hint,
                 skip: skip,
                 limit: limit,
                 batch_size: batch_size,
                 single_batch: single_batch,
                 max_time_ms: max_time_ms.try &.to_i64,
                 collation: collation,
                 session: session,
                 allow_disk_use: allow_disk_use
               )
             when "findOne"
               collection.find_one(
                 filter: filter.not_nil!,
                 sort: sort,
                 projection: projection,
                 hint: hint,
                 skip: skip,
                 collation: collation,
                 session: session
               )
             when "aggregate"
               if operation_object == "database"
                 db.aggregate(
                   pipeline: pipeline.not_nil!,
                   allow_disk_use: allow_disk_use,
                   batch_size: batch_size,
                   max_time_ms: max_time_ms,
                   bypass_document_validation: bypass_document_validation,
                   read_concern: read_concern,
                   collation: collation,
                   hint: hint,
                   write_concern: write_concern,
                   session: session
                 )
               else
                 collection.aggregate(
                   pipeline: pipeline.not_nil!,
                   allow_disk_use: allow_disk_use,
                   batch_size: batch_size,
                   max_time_ms: max_time_ms,
                   bypass_document_validation: bypass_document_validation,
                   read_concern: read_concern,
                   collation: collation,
                   hint: hint,
                   write_concern: write_concern,
                   session: session
                 )
               end
             when "watch"
               if operation_object == "database"
                 db.watch
               elsif operation_object == "collection"
                 collection.watch
               else
                 client.watch
               end
             when "updateOne"
               collection.update_one(
                 filter: filter.not_nil!,
                 update: update.not_nil!,
                 upsert: upsert,
                 array_filters: array_filters,
                 collation: collation,
                 hint: hint,
                 ordered: ordered,
                 write_concern: write_concern,
                 bypass_document_validation: bypass_document_validation,
                 session: session
               )
             when "updateMany"
               collection.update_many(
                 filter: filter.not_nil!,
                 update: update.not_nil!,
                 upsert: upsert,
                 array_filters: array_filters,
                 collation: collation,
                 hint: hint,
                 ordered: ordered,
                 write_concern: write_concern,
                 bypass_document_validation: bypass_document_validation,
                 session: session
               )
             when "replaceOne"
               collection.replace_one(
                 filter: filter.not_nil!,
                 replacement: replacement.not_nil!,
                 upsert: upsert,
                 collation: collation,
                 hint: hint,
                 ordered: ordered,
                 write_concern: write_concern,
                 bypass_document_validation: bypass_document_validation,
                 session: session
               )
             when "insertOne"
               collection.insert_one(
                 document: document.not_nil!,
                 write_concern: write_concern,
                 bypass_document_validation: bypass_document_validation,
                 session: session
               )
             when "insertMany"
               collection.insert_many(
                 documents: documents.not_nil!,
                 ordered: ordered,
                 write_concern: write_concern,
                 bypass_document_validation: bypass_document_validation,
                 session: session
               )
             when "deleteOne"
               collection.delete_one(
                 filter: filter.not_nil!,
                 collation: collation,
                 hint: hint,
                 ordered: ordered,
                 write_concern: write_concern,
                 session: session
               )
             when "deleteMany"
               collection.delete_many(
                 filter: filter.not_nil!,
                 collation: collation,
                 hint: hint,
                 ordered: ordered,
                 write_concern: write_concern,
                 session: session
               )
             when "findOneAndUpdate"
               collection.find_one_and_update(
                 filter: filter.not_nil!,
                 update: update.not_nil!,
                 sort: sort,
                 new: new_,
                 fields: fields,
                 upsert: upsert,
                 bypass_document_validation: bypass_document_validation,
                 write_concern: write_concern,
                 collation: collation,
                 array_filters: array_filters,
                 hint: hint,
                 max_time_ms: max_time_ms,
                 session: session
               )
             when "findOneAndReplace"
               collection.find_one_and_replace(
                 filter: filter.not_nil!,
                 replacement: replacement.not_nil!,
                 sort: sort,
                 new: new_,
                 fields: fields,
                 upsert: upsert,
                 bypass_document_validation: bypass_document_validation,
                 write_concern: write_concern,
                 collation: collation,
                 array_filters: array_filters,
                 hint: hint,
                 max_time_ms: max_time_ms,
                 session: session
               )
             when "findOneAndDelete"
               collection.find_one_and_delete(
                 filter: filter.not_nil!,
                 sort: sort,
                 fields: fields,
                 bypass_document_validation: bypass_document_validation,
                 write_concern: write_concern,
                 collation: collation,
                 hint: hint,
                 max_time_ms: max_time_ms,
                 session: session
               )
             when "listDatabases"
               client.list_databases
             when "listCollections"
               db.list_collections
             when "listIndexes"
               collection.list_indexes
             when "listCollectionNames"
               # not implemented
               return
             when "listDatabaseNames"
               # not implemented
               return
             when "listIndexNames"
               # not implemented
               return
             when "mapReduce"
               # not implemented
               return
             when "listCollectionObjects"
               # not implemented
               return
             when "listDatabaseObjects"
               # not implemented
               return
             when "bulkWrite"
               requests = Array(Mongo::Bulk::WriteModel).new
               arguments["requests"].as_a.each { |req|
                 name = req["name"].as_s
                 arguments = req["arguments"].as_h
                 collation = arguments["collation"]?.try { |c|
                   Mongo::Collation.from_bson(BSON.from_json(c.to_json))
                 }
                 hint = arguments["hint"]?.try { |h|
                   next h.as_s if h.as_s?
                   BSON.from_json(h.to_json)
                 }
                 case name
                 when "insertOne"
                   requests << Mongo::Bulk::InsertOne.new(
                     document: bson_arg("document").not_nil!
                   )
                 when "deleteOne"
                   requests << Mongo::Bulk::DeleteOne.new(
                     filter: bson_arg("filter").not_nil!,
                     collation: collation,
                     hint: hint
                   )
                 when "deleteMany"
                   requests << Mongo::Bulk::DeleteMany.new(
                     filter: bson_arg("filter").not_nil!,
                     collation: collation,
                     hint: hint
                   )
                 when "replaceOne"
                   requests << Mongo::Bulk::ReplaceOne.new(
                     filter: bson_arg("filter").not_nil!,
                     replacement: bson_arg("replacement").not_nil!,
                     collation: collation,
                     hint: hint,
                     upsert: bool_arg("upsert")
                   )
                 when "updateOne"
                   requests << Mongo::Bulk::UpdateOne.new(
                     filter: bson_arg("filter").not_nil!,
                     update: bson_or_array_arg("update").not_nil!,
                     array_filters: bson_array_arg("arrayFilters"),
                     collation: collation,
                     hint: hint,
                     upsert: bool_arg("upsert")
                   )
                 when "updateMany"
                   requests << Mongo::Bulk::UpdateMany.new(
                     filter: bson_arg("filter").not_nil!,
                     update: bson_or_array_arg("update").not_nil!,
                     array_filters: bson_array_arg("arrayFilters"),
                     collation: collation,
                     hint: hint,
                     upsert: bool_arg("upsert")
                   )
                 else
                   puts "Not supported (bulk): #{name}"
                 end
               }
               collection.bulk_write(
                 requests: requests,
                 ordered: ordered,
                 bypass_document_validation: bypass_document_validation,
                 session: session
               )
             else
               puts "Not supported: #{operation_name}"
             end

    # Validation
    if outcome_result
      if result.is_a? BSON
        begin
          compare_json(outcome_result, JSON.parse(result.to_json))
        rescue e
          puts "Error while comparing outcome json fields.\nOutcome: #{outcome_result.to_json}\nResult: #{result.to_json}"
          raise e
        end
      elsif result.is_a? Array
        result.zip(outcome_result.as_a) { |v1, v2|
          v1.to_s.should eq v2.to_s
        }
      elsif result.is_a? Mongo::Cursor
        results = result.map { |elt| elt }.to_a
        results.zip(outcome_result.as_a) { |v1, v2|
          v1.to_json.should eq v2.to_json
        }
      elsif result.is_a? Mongo::Commands::Common::UpdateResult
        matched_count = outcome_result["matchedCount"].as_i
        modified_count = outcome_result["modifiedCount"].as_i
        upserted_count = outcome_result["upsertedCount"].as_i
        upserted_id = outcome_result["upsertedId"]?
        result_upserted_count = result.upserted.try(&.size) || 0
        result_upserted_id = result.upserted.try &.[0]?.try &._id
        ((result.n || 0) - result_upserted_count).should eq matched_count
        (result.n_modified || 0).should eq modified_count
        result_upserted_count.should eq upserted_count
        result_upserted_id.should eq upserted_id
      elsif result.is_a? Mongo::Commands::Common::InsertResult
        inserted_count =
          (outcome_result["insertedCount"]?.try &.as_i) ||
            (outcome_result["insertedId"]?.try { 1 }) ||
            (outcome_result["insertedIds"].as_h.size)
        result.n.should eq inserted_count
      elsif result.is_a? Mongo::Commands::Common::DeleteResult
        deleted_count = outcome_result["deletedCount"]?.try(&.as_i) || 0
        result.n.should eq deleted_count
      elsif result.is_a? Mongo::Bulk::WriteResult
        result.n_inserted.should eq (outcome_result["insertedCount"]? || 0)
        result.n_matched.should eq (outcome_result["matchedCount"]? || 0)
        result.n_modified.should eq (outcome_result["modifiedCount"]? || 0)
        result.n_removed.should eq (outcome_result["deletedCount"]? || 0)
        result.n_upserted.should eq (outcome_result["upsertedCount"]? || 0)
      elsif result.responds_to? :to_bson
        result.to_bson.to_json.should eq outcome_result.to_json
      else
        result.to_json.should eq outcome_result.to_json
      end
    end

    result
  end

  def validate_outcome(outcome, database, collection)
    # Validation
    if outcome && (outcome_data = outcome.dig?("collection", "data").try &.as_a)
      outcome_collection_name = outcome.dig?("collection", "name").try &.as_s
      outcome_collection = outcome_collection_name ? database[outcome_collection_name] : collection
      collection_data = outcome_collection.find(
        sort: {_id: 1},
        read_preference: Mongo::ReadPreference.new(mode: "primary"),
        read_concern: Mongo::ReadConcern.new("local")
      ).to_a
      collection_data.to_json.should eq outcome_data.to_json
    end
  end

  def validate_expectations(expectations, counter_ref, client, sessions = nil, *, lsid_list = [] of BSON, command_started_only = false, ignore_cursor_42s = false)
    cursor_id = nil

    client.subscribe_commands do |event|
      event = event.as(Mongo::Monitoring::Commands::Event)
      counter = counter_ref.value
      next if expectations.try &.size.<= counter
      expectation = expectations.try &.[counter].as_h

      result = case event
               when Mongo::Monitoring::Commands::CommandStartedEvent
                 if lsid = event.command["lsid"]?
                   lsid_list << lsid.as(BSON)
                 end
                 expectation.try &.["command_started_event"]?
               when Mongo::Monitoring::Commands::CommandSucceededEvent
                 next if command_started_only
                 expectation.try &.["command_succeeded_event"]?
               when Mongo::Monitoring::Commands::CommandFailedEvent
                 next if command_started_only
                 expectation.try &.["command_failed_event"]?
               end
      next if event.command_name == "configureFailPoint"

      counter_ref.value += 1

      next unless result

      result = result.not_nil!

      if command_name = result["command_name"]?
        event.command_name.should eq command_name
      end

      case event
      when Mongo::Monitoring::Commands::CommandStartedEvent
        event.database_name.should eq result["database_name"] if result["database_name"]?
        begin
          self.compare_json(result["command"], JSON.parse(event.command.to_json)) do |a, b, key|
            if key == "afterClusterTime" && a == 42
              b.should_not be_nil
            elsif key == "recoveryToken" && a == 42
              b.as_h?.should_not be_nil
            elsif key == "getMore" && a == 42
              if ignore_cursor_42s
                # ignore
              else
                b.should eq cursor_id
              end
            elsif a == 42
              b.should eq cursor_id
            elsif (s = sessions) && a == "session0"
              b.to_json.should eq s[0].session_id.to_bson.to_json
            elsif (s = sessions) && a == "session1"
              b.to_json.should eq s[1].session_id.to_bson.to_json
            else
              a.should eq b
            end
          end
        rescue e
          puts "Error while comparing json fields (CommandStartedEvent).\nExpected: #{result["command"].to_json}\nReceived: #{event.command.to_json}"
          raise e
        end
      when Mongo::Monitoring::Commands::CommandSucceededEvent
        begin
          self.compare_json(result["reply"], JSON.parse(event.reply.to_json)) do |a, b, _key|
            if a == 42
              b.should_not be_nil
              cursor_id = b.as_i64
            elsif a == ""
              b.should_not be_nil
            else
              a.should eq b
            end
          end
        rescue e
          puts "Error while comparing json fields (CommandSucceededEvent).\nExpected: #{result["reply"].to_json}\nReceived: #{event.reply.to_json}"
          raise e
        end
      when Mongo::Monitoring::Commands::CommandFailedEvent
        # nothing special to do
      end
    rescue e
      # puts e.inspect_with_backtrace
      raise e
    end
  end
end
