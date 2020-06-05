require "bson"
require "socket"
require "openssl"
require "../uri"
require "./client"
require "../sdam/**"

abstract class Mongo::Client
  class Single < Client

    getter! topology : SDAM::TopologyDescription
    getter options : Options

    @@mutex = Mutex.new
    @sockets : Hash(SDAM::ServerDescription, IO) =  Hash(SDAM::ServerDescription, IO).new
    @socket_check_interval : Time::Span = 5.seconds
    @last_scan : Time = Time::UNIX_EPOCH

    @monitor : SDAM::Monitor::Global? = nil
    @min_heartbeat_frequency : Time::Span = 500.milliseconds

    def initialize(connection_string : String = "mongodb://localhost:27017")
      seeds, @options = Mongo::URI.parse(connection_string)

      if read_pref = @options.read_preference
        @read_preference = ReadPreference.new(
          mode: read_pref,
          max_staleness_seconds: @options.max_staleness_seconds,
          tags: @options.read_preference_tags.map { |tags|
            bson = BSON.new
            tags.split(',').each { |tag|
              key, value = tag.split(':')
              bson[key] = value
            }
            bson
          }
        )
      end

      @topology = SDAM::TopologyDescription.new(self, seeds.map(&.address), @options)
      @monitor = SDAM::Monitor::Global.new(self)
    end

    def server_selection(command, args, read_preference : ReadPreference) : SDAM::ServerDescription
      # See: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#single-threaded-server-selection
      selection_start_time = Time.utc
      selection_timeout = selection_start_time + @options.server_selection_timeout
      topology.stale = Time.utc - @last_scan > @options.heartbeat_frequency

      loop do
        if topology.stale
          target_scan_time = @last_scan + @min_heartbeat_frequency
          if !@options.server_selection_try_once && target_scan_time > selection_timeout
            raise ServerSelectionError.new "Timeout (#{@options.server_selection_timeout}) reached while trying to select a suitable server."
          end
          if Time.utc < target_scan_time
            sleep(Time.utc - target_scan_time)
          end
          @last_scan = Time.utc
          topology.stale = false
          @monitor.try &.scan
        end

        unless topology.compatible
          raise Mongo::Error.new topology.compatibility_error
        end

        # Find suitable servers by topology type and operation type
        # Filter the suitable servers by calling the optional, application-provided server selector.
        # If there are any suitable servers, choose one at random from those within the latency window and return it; otherwise, mark the topology stale and continue to step #8
        suitable_servers = find_suitable_servers(command, args, read_preference)
        selected_server = suitable_servers.try { |s| select_by_latency(s) }

        return selected_server if selected_server

        topology.stale = true

        # If serverSelectionTryOnce is true and the last scan time is newer than the selection start time, raise a server selection error; otherwise, goto Step #4
        if @options.server_selection_try_once && @last_scan > selection_start_time
          raise Mongo::Error.new "No server available for query with ReadPreference #{read_preference.mode}."
        end

        # If the current time exceeds the maximum time, raise a server selection error
        if Time.utc > selection_timeout
          raise ServerSelectionError.new "Timeout (#{@options.server_selection_timeout}) reached while trying to select a suitable server."
        end
      end
    end

    def command(command cmd, ignore_errors = false, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, **args)
      @@mutex.synchronize {
        super(cmd,  **args, ignore_errors: ignore_errors, write_concern: write_concern, read_concern: read_concern)
      }
    end

    def get_connection(server_description : SDAM::ServerDescription) : IO
      if socket = @sockets[server_description]?
        return socket
      end

      if server_description.address.ends_with? ".sock"
        socket = UNIXSocket.new(server_description.address)
      else
        split = server_description.address.split(':')
        socket = TCPSocket.new(split[0], split[1]? || 27017)
      end

      if @options.ssl || @options.tls
        context = OpenSSL::SSL::Context::Client.new
        if tls_ca_file = @options.tls_ca_file
          context.ca_certificates = tls_ca_file
        end
        if @options.tls_insecure || @options.tls_allow_invalid_certificates
          context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
        end
        context.add_options(OpenSSL::SSL::Options::ALL)
        context.add_options(OpenSSL::SSL::Options.flags(
          NO_SSL_V2,
          NO_COMPRESSION,
          NO_SESSION_RESUMPTION_ON_RENEGOTIATION
        ))
        socket = OpenSSL::SSL::Socket::Client.new(socket, context, sync_close: true)
      end

      @sockets[server_description] = socket
      socket
    end

    def close_connection(server_description : SDAM::ServerDescription)
      @sockets.delete(server_description).try &.close
    end

    def release_connection(server_description : SDAM::ServerDescription, socket : IO)
    end

    def on_topology_update
      # Do nothing special
    end
  end

  def self.new(address : String = "mongodb://localhost:27017")
    Single.new(address)
  end
end
