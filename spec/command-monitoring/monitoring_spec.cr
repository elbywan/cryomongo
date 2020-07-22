require "../spec_helper"

describe Mongo::Monitoring do
  with_mongo { |get_client, topology|
    Dir.glob "./spec/command-monitoring/tests/*.json" do |file_path|
      test_file = JSON.parse(File.open(file_path) { |file|
        file.gets_to_end
      })

      tests = test_file["tests"].as_a
      data = test_file["data"]?.try &.as_a.map { |elt| BSON.from_json(elt.to_json) }
      collection_name = test_file["collection_name"]?.try(&.as_s) || "collection"
      database_name = test_file["database_name"]?.try(&.as_s) || "database"

      context "#{file_path}" do
        tests.each { |test|
          next if test["ignore"]?
          next if test["ignore_if_server_version_greater_than"]?.try { |v| semantic(v.as_s) < SERVER_VERSION }
          next if test["ignore_if_server_version_less_than"]?.try { |v| semantic(v.as_s) > SERVER_VERSION }
          if topologies = test["ignore_if_topology_type"]?.try &.as_a
            next if topology.to_s.underscore.in? topologies
          end

          description = test["description"].as_s
          focus = test["focus"]?.try(&.as_bool) || false
          operation = test["operation"].as_h
          expectations = test["expectations"].as_a
          counter = 0

          it "#{description} (#{file_path})", focus: focus do
            client = get_client.call
            local_client = Mongo::Client.new
            global_database = client[database_name]
            local_database = local_client[database_name]
            collection = local_client[database_name][collection_name]

            global_database[collection_name].delete_many(BSON.new)
            data.try { |d| global_database[collection_name].insert_many(d) if d.size > 0 }

            if collection_options = operation["collectionOptions"]?
              if read_concern = collection_options["readConcern"]?
                collection.read_concern = Mongo::ReadConcern.from_bson(BSON.from_json(read_concern.to_json))
              end
              if write_concern = collection_options["writeConcern"]?
                collection.write_concern = Mongo::WriteConcern.from_bson(BSON.from_json(write_concern.to_json))
              end
            end

            cursor_id = nil

            subscription = local_client.subscribe do |event|
              expectation = expectations[counter].as_h
              counter += 1

              result = case event
                when Mongo::Monitoring::CommandStartedEvent
                  expectation["command_started_event"]?
                when Mongo::Monitoring::CommandSucceededEvent
                  expectation["command_succeeded_event"]?
                when Mongo::Monitoring::CommandFailedEvent
                  expectation["command_failed_event"]?
              end

              result.should_not be_nil
              result = result.not_nil!

              event.command_name.should eq result["command_name"]

              case event
              when Mongo::Monitoring::CommandStartedEvent
                event.database_name.should eq result["database_name"]
                Runner.compare_json(result["command"], JSON.parse(event.command.to_json)) { |a, b|
                  if a == 42
                    b.should eq cursor_id
                  else
                    a.should eq b
                  end
                }
              when Mongo::Monitoring::CommandSucceededEvent
                Runner.compare_json(result["reply"], JSON.parse(event.reply.to_json)) { |a, b|
                  if a == 42
                    b.should_not be_nil
                    cursor_id = b.as_i64
                  elsif a == ""
                    b.should_not be_nil
                  else
                    a.should eq b
                  end
                }
              when Mongo::Monitoring::CommandFailedEvent
                # nothing special to do
              end
            rescue e
              puts e.inspect_with_backtrace
              raise e
            end

            begin
              result = Runner.spec_operation(client, local_database, collection, operation)
              if result.is_a? Mongo::Cursor
                result.to_a
              end
            rescue e
              # puts "Error while running test #{description} (#{file_path}): #{e.inspect_with_backtrace}"
            end

            counter.should eq expectations.size
          ensure
            subscription.try { |s| local_client.try &.unsubscribe(s) }
            local_client.try &.close
          end
        }
      end
    end
  }
end
