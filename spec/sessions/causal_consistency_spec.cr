require "../spec_helper"

describe Mongo::Session do
  with_mongo(MongoLaunchTopology::Replicaset) { |_get_client, _topology|
    # The causal consistency test plan.
    # See: https://github.com/mongodb/specifications/blob/master/source/causal-consistency/causal-consistency.rst#test-plan
    context "causal consistency test plan" do
      it "the operationTime should not have a value when a ClientSession is first created" do
        session = Mongo::Client.new.start_session
        session.operation_time.should be_nil
      end

      it "the first read in a causally consistent session must not send afterClusterTime to the server" do
        client = Mongo::Client.new
        session = client.start_session
        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandStartedEvent
            event.command["readConcern"]?.should be_nil
          end
        }
        client["db"]["coll"].find(session: session)
      end

      it "the first read or write on a ClientSession should update the operationTime of the ClientSession, even if there is an error" do
        client = Mongo::Client.new
        session = client.start_session

        operation_time = nil

        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandSucceededEvent
            operation_time = event.reply["operationTime"]?
          when Mongo::Monitoring::Commands::CommandFailedEvent
            operation_time = event.reply["operationTime"]?
          end
        }
        client["db"]["coll"].insert_one({ a: 1 }, session: session)
        session.operation_time.should eq operation_time
        begin client["db"]["coll"].find({ a: 1 }, limit: -1, session: session); rescue; end
        session.operation_time.should eq operation_time
      end

      it "a findOne followed by any other read operation should include the operationTime returned by the server for the first operation in the afterClusterTime parameter of the second operation" do
        client = Mongo::Client.new
        session = client.start_session

        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandStartedEvent
            if event.command["count"]?
              event.command["readConcern"]?.try(&.as(BSON).["afterClusterTime"]?).should eq session.operation_time
            end
          end
        }
        client["db"]["coll"].find_one({ a: 1 }, session: session)
        client["db"]["coll"].estimated_document_count(session: session)
      end

      it "any write operation followed by a findOne operation should include the operationTime of the first operation in the afterClusterTime parameter of the second operation, including the case where the first operation returned an error" do
        client = Mongo::Client.new
        session = client.start_session

        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandStartedEvent
            if event.command["count"]?
              event.command["readConcern"]?.try &.as(BSON).["afterClusterTime"]?.should eq session.operation_time
            end
          end
        }
        client["db"]["coll"].insert_one({ a: 1 }, session: session)
        client["db"]["coll"].estimated_document_count(session: session)
      end

      it "a read operation in a ClientSession that is not causally consistent should not include the afterClusterTime parameter in the command sent to the server" do
        client = Mongo::Client.new
        session = client.start_session

        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandStartedEvent
              event.command["readConcern"]?.should be_nil
          end
        }
        client["db"]["coll"].stats(session: session)
      end

      it "when using the default server ReadConcern the readConcern parameter in the command sent to the server should not include a level field" do
        client = Mongo::Client.new
        session = client.start_session

        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandStartedEvent
            if event.command["count"]?
              event.command["readConcern"]?.try &.as(BSON).["level"]?.should be_nil
            end
          end
        }
        client["db"]["coll"].insert_one({ a: 1 }, session: session)
        client["db"]["coll"].estimated_document_count(session: session)
      end

      it "when using a custom ReadConcern the readConcern field in the command sent to the server should be a merger of the ReadConcern value and the afterClusterTime field" do
        client = Mongo::Client.new
        session = client.start_session
        client.read_concern = Mongo::ReadConcern.new(level: "majority")

        client.subscribe_commands { |event|
          case event
          when Mongo::Monitoring::Commands::CommandStartedEvent
            if event.command["count"]?
              event.command["readConcern"]?.try &.as(BSON).["level"]?.should eq "majority"
            end
          end
        }
        client["db"]["coll"].insert_one({ a: 1 }, session: session)
        client["db"]["coll"].estimated_document_count(session: session)
      end
    end
  }
end
