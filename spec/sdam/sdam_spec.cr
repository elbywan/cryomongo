require "json"
require "../spec_helper"

alias ServerDescription = Mongo::SDAM::ServerDescription
alias Topology = Mongo::SDAM::TopologyDescription

struct Mongo::Connection
  def initialize(@server_description : ServerDescription, @socket : IO, @credentials : Mongo::Credentials, @options : Mongo::Options)
  end
end

class Mongo::SDAM::Monitor::Mock < Mongo::SDAM::Monitor
  def get_connection(server_description : ServerDescription) : Mongo::Connection
    @client.get_connection(server_description)
  end

  def close_connection(server_description : ServerDescription)
  end
end

class Mongo::Client::Mock < Mongo::Client
  property mocks : Hash(ServerDescription, BSON) = Hash(ServerDescription, BSON).new

  def reset_mocks
    @mocks = Hash(ServerDescription, BSON).new
  end

  def add_mock(address, mock)
    description = topology.servers.find &.address.== address
    description.try { |d| mocks[d] = mock }
  end

  def get_connection(server_description : SDAM::ServerDescription) : Mongo::Connection
    mock = mocks[server_description]

    raise Socket::Error.new "Simulated network error" if mock.empty?

    op_msg = Messages::OpMsg.new(mock)
    full_msg = Messages::Message.new(op_msg)

    writer = IO::Memory.new
    reader = IO::Memory.new
    full_msg.to_io(reader)
    reader.rewind
    io = IO::Stapled.new reader, writer
    Mongo::Connection.new(server_description, io, @credentials, @options)
  end

  def server_description(address : String)
    topology.servers.find(&.address.== address)
  end

  def check_server(server_description)
    @monitors.find(&.server_description.address.== server_description.address).not_nil!.check(server_description)
  end

  def add_monitor(server_description : SDAM::ServerDescription, *, start_monitoring = true)
    monitor = SDAM::Monitor::Mock.new(self, server_description, @credentials, @options.heartbeat_frequency || 10.seconds)
    @monitors << monitor
  end
end

describe Mongo::SDAM do
  %w(
    single
    sharded
    rs
  ).each { |suite|
    context "[#{suite}]" do
      Dir.glob "./spec/sdam/tests/#{suite}/*.json" do |file_path|
        test = JSON.parse(File.open(file_path) { |file|
          file.gets_to_end
        })

        description = test["description"].as_s
        focus = test["focus"]?.try(&.as_bool) || false

        it "#{description} (#{file_path})", focus: focus do
          uri = test["uri"].as_s
          client = Mongo::Client::Mock.new(uri, start_monitoring: false)
          phases = test["phases"].as_a
          phases.each { |phase|
            client.reset_mocks
            responses = phase["responses"].as_a?
            responses.try &.each { |response|
              address = response[0].as_s
              is_master_payload = BSON.from_json(response[1].to_json)
              server_description = client.server_description(address)
              client.add_mock(address, is_master_payload)
              server_description.try { |sd|
                new_description = client.check_server(sd)
                new_description.try { |nd| client.topology.update(sd, nd) }
              }
            }
            outcome = phase["outcome"].as_h
            client.topology.tap { |topology|
              topology.type.should eq Topology::TopologyType.parse(outcome["topologyType"].as_s)
              topology.set_name.should eq outcome["setName"]
              outcome_servers = outcome["servers"].as_h
              outcome_servers.each { |address, server|
                server = server.as_h
                topology_server = client.topology.servers.find(&.address.== address.downcase).not_nil!
                topology_server.type.should eq ServerDescription::ServerType.parse(server["type"].as_s)
                topology_server.set_name.should eq server["setName"] if server.has_key?("setName")
                topology_server.set_version.should eq server["setVersion"] if server.has_key?("setVersion")
                if server.has_key?("electionId")
                  election_id = server["electionId"].try { |oid|
                    id = oid.as_h?.try &.["$oid"]?.try &.as_s
                    id ? BSON::ObjectId.new(id) : nil
                  }
                  topology_server.election_id.should eq election_id
                end
                topology_server.logical_session_timeout_minutes.should eq server["logicalSessionTimeoutMinutes"] if server.has_key?("logicalSessionTimeoutMinutes")
                topology_server.min_wire_version.should eq server["minWireVersion"] if server.has_key?("minWireVersion")
                topology_server.max_wire_version.should eq server["maxWireVersion"] if server.has_key?("maxWireVersion")
                # topology_server.topology_version.should eq server["topologyVersion"] if server.has_key?("topologyVersion")
                # topology_server.pool.should eq server["pool"] if server.has_key?("pool")
              }
              topology.logical_session_timeout_minutes.should eq outcome["logicalSessionTimeoutMinutes"]?
              topology.max_set_version.should eq outcome["maxSetVersion"]?
              if outcome.has_key?("maxElectionId")
                max_election_id = outcome["maxElectionId"]?.try { |eid|
                  BSON::ObjectId.new(eid["$oid"].as_s)
                }
                topology.max_election_id.should eq max_election_id
              end
              topology.compatible.should eq outcome["compatible"]? if outcome.has_key? "compatible"
            }
          }
        end
      end
    end
  }
end
