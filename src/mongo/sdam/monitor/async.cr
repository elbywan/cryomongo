require "./monitor"

require "../../commands/replication/is_master"
require "../../messages/message"

module Mongo::SDAM
  class Monitor::Async < Monitor

    getter resume_scan = Channel(Nil).new
    getter server_description : ServerDescription
    @connection : Mongo::Connection? = nil
    @finalized : Bool = false

    def initialize(
      @client : Mongo::Client::Async,
      @server_description : ServerDescription,
      @credentials : Mongo::Credentials,
      @heartbeat_frequency  : Time::Span = 10.seconds
    )
      super(@client.topology, @heartbeat_frequency)
    end

    def get_connection(server_description : ServerDescription) : IO
      if !@connection || @connection.try &.socket.closed?
        @connection = Mongo::Connection.new(@server_description, @credentials, @client.options)
      end
      @connection.not_nil!.socket
    end

    def close_connection(server_description : ServerDescription)
      if (connection = @connection) && !connection.socket.closed?
        connection.socket.close
      end
      @connection = nil
      @client.close_connection_pool(server_description)
    end

    def scan
      spawn {
        loop do
          # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#multi-threaded-or-asynchronous-monitoring
          before_cooldown = Time.utc - @cooldown
          server_to_check = @topology.servers.find(&.address.== @server_description.address)

          break if server_to_check.nil? || @finalized

          unless (new_description = check(server_to_check)).nil?
            @topology.update(server_to_check, new_description)
            @client.on_topology_update
          end

          select
          when resume_scan.receive
            # Immediate scan requested
            sleep(Time.utc - before_cooldown) if Time.utc < before_cooldown
          when timeout @heartbeat_frequency
          end
        rescue e
          # Monitoring error
        end
        close_connection(@server_description)
        @client.stop_monitoring(@server_description)
      }
      Fiber.yield
    end

    def request_immediate_scan
      select
      when resume_scan.send nil
      else
        # Ignore - scan is in progress already
      end
    end

    def close
      @finalized = true
    end

    def finalize
      @finalized = true
    end
  end
end
