require "../spec_helper"
require "./spec_helper"

include Crud::Helpers

describe "Mongo CRUD" do
  with_mongo { |get_client|
    %w(
      v1
      v2
    ).each do |version|
      context "[#{version}]" do
        Dir.glob "./spec/crud/tests/#{version}/**/*.json" do |file_path|
          test_file = JSON.parse(File.open(file_path) { |file|
            file.gets_to_end
          })

          tests = test_file["tests"].as_a
          data = test_file["data"]?.try &.as_a.map { |elt| BSON.from_json(elt.to_json) }
          version_root = test_file["runOn"]?.try(&.as_a.[0]) || test_file
          min_server_version = version_root["minServerVersion"]?.try { |v| semantic(v.as_s) } || SERVER_VERSION
          max_server_version = version_root["maxServerVersion"]?.try { |v| semantic(v.as_s) } || SERVER_VERSION
          collection_name = test_file["collection_name"]?.try(&.as_s) || "collection"
          database_name = test_file["database_name"]?.try(&.as_s) || "database"

          if max_server_version < SERVER_VERSION || min_server_version > SERVER_VERSION
            if max_server_version < SERVER_VERSION
              # puts "(#{file_path}): maximum version does not match the mongodb running instance: #{max_server_version} < #{SERVER_VERSION}"
            else
              # puts "(#{file_path}): minimum version does not match the mongodb running instance: #{min_server_version} > #{SERVER_VERSION}"
            end
            next
          end

          context "#{file_path}" do
            tests.each { |test|
              client_options = test["clientOptions"]?.try &.as_h
              uri = "mongodb://localhost:27017"
              if client_options
                uri += "?" + query_options(client_options)
              end

              description = test["description"].as_s
              focus = test["focus"]?.try(&.as_bool) || false

              it "#{description} (#{file_path})", focus: focus do
                client = get_client.call
                local_client = Mongo::Client.new(uri)
                global_database = client[database_name]
                local_database = local_client[database_name]
                global_database[collection_name].delete_many(BSON.new)
                data.try { |d| global_database[collection_name].insert_many(d) if d.size > 0 }

                operation = test["operation"]?.try &.as_h
                test_outcome = test["outcome"]?.try &.as_h
                operations = operation.try { |o| [o] } || test["operations"].as_a.map(&.as_h)
                operations.each { |o|
                  expect_error = o["error"]?.try &.as_bool || test_outcome.try &.["error"]?.try &.as_bool
                  collection = local_client[database_name][collection_name]

                  if collection_options = o["collectionOptions"]?
                    if read_concern = collection_options["readConcern"]?
                      collection.read_concern = Mongo::ReadConcern.from_bson(BSON.from_json(read_concern.to_json))
                    end
                    if write_concern = collection_options["writeConcern"]?
                      collection.write_concern = Mongo::WriteConcern.from_bson(BSON.from_json(write_concern.to_json))
                    end
                  end

                  if expect_error
                    expect_raises(Exception) {
                      spec_operation(global_database, local_database, collection, o, test_outcome)
                    }
                  else
                    spec_operation(global_database, local_database, collection, o, test_outcome)
                  end
                }
              ensure
                local_client.try &.close
              end
            }
          end
        end
      end
    end
  }
end
