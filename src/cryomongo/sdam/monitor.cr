require "socket"

# :nodoc:
module Mongo::SDAM
  class Monitor
    getter resume_scan = Channel(Nil).new
    getter server_description : ServerDescription

    @heartbeat_frequency : Time::Span = 10.seconds
    @topology : TopologyDescription
    @connection : Mongo::Connection? = nil
    @closed : Bool = false
    @scan_started : Bool = false

    def initialize(
      @client : Mongo::Client,
      @server_description : ServerDescription,
      @credentials : Mongo::Credentials,
      @heartbeat_frequency : Time::Span = 10.seconds
    )
      @topology = @client.topology
    end

    def get_connection(server_description : ServerDescription) : Mongo::Connection
      if !@connection || @connection.try &.socket.closed?
        @connection = Mongo::Connection.new(@server_description, @credentials, @client.options)
        @connection.try &.handshake(send_metadata: true, appname: @client.options.appname)
      end
      @connection.not_nil!
    end

    def close_connection(server_description : ServerDescription)
      if (connection = @connection) && !connection.socket.closed?
        connection.socket.close
      end
      @connection = nil
      @client.close_connection_pool(server_description)
    end

    def scan
      return if @scan_started
      @scan_started = true
      loop do
        break if @closed
        # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#multi-threaded-or-asynchronous-monitoring
        before_cooldown = Time.utc + @client.min_heartbeat_frequency
        server_to_check = @topology.servers.find(&.address.== @server_description.address)

        break if server_to_check.nil? || @closed

        unless (new_description = check(server_to_check)).nil?
          @topology.update(server_to_check, new_description)
        end

        select
        when resume_scan.receive
          # Immediate scan requested
          sleep(before_cooldown - Time.utc) if Time.utc < before_cooldown
        when timeout @heartbeat_frequency
        end
      rescue e
        Mongo::Log.error { "Monitoring error: #{e}" }
        # Monitoring error
      end
      close_connection(@server_description)
      @client.stop_monitoring(@server_description)
    end

    def request_immediate_scan
      select
      when resume_scan.send nil
        # Fiber.yield
      else # Ignore - scan is in progress already
      end
    end

    def check(server_description : ServerDescription)
      server_description.last_update_time = Time.utc
      connection = get_connection(server_description)
      result, round_trip_time = connection.handshake
      new_rtt = Connection.average_round_trip_time(round_trip_time, server_description.round_trip_time)
      ServerDescription.new(server_description.address, result, new_rtt)
    rescue error : Exception
      Mongo::Log.error { "Monitoring handshake error: #{error}" }
      # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#network-or-command-error-during-server-check
      known_state = !server_description.type.unknown?
      description = ServerDescription.new(server_description.address)
      description.error = error.message
      description.last_update_time = server_description.last_update_time
      close_connection(server_description)
      if known_state && error.is_a? Client::NetworkError
        check(description)
      else
        description
      end
    end

    def close
      @closed = true
      request_immediate_scan
      Fiber.yield
    end
  end
end
