require "bson"
require "socket"
require "openssl"
require "db"
require "../uri"
require "./client"
require "../sdam/**"
require "../connection"

abstract class Mongo::Client
  class Async < Client

    getter! topology : SDAM::TopologyDescription
    getter options : Options

    @@lock = Mutex.new
    @pools : Hash(SDAM::ServerDescription, DB::Pool(Mongo::Connection)) = Hash(SDAM::ServerDescription, DB::Pool(Mongo::Connection)).new
    @monitors : Array(SDAM::Monitor::Async) = Array(SDAM::Monitor::Async).new
    @socket_check_interval : Time::Span = 5.seconds
    @last_scan : Time = Time::UNIX_EPOCH
    @min_heartbeat_frequency : Time::Span = 500.milliseconds
    @topology_update = Channel(Nil).new

    def initialize(connection_string : String = "mongodb://localhost:27017", *, start_monitoring = true)
      seeds, @options, @credentials = Mongo::URI.parse(connection_string)

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

      topology = SDAM::TopologyDescription.new(self, seeds.map(&.address), @options)
      topology.servers.each { |server|
        monitor = SDAM::Monitor::Async.new(self, server, @credentials, @options.heartbeat_frequency || 10.seconds)
        @monitors << monitor
        monitor.scan if start_monitoring
      }
    end

    def server_selection(command, args, read_preference : ReadPreference) : SDAM::ServerDescription
      # See: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#multi-threaded-or-asynchronous-server-selection
      selection_start_time = Time.utc
      selection_timeout = selection_start_time + @options.server_selection_timeout

      loop do
        unless topology.compatible
          raise Mongo::Error.new topology.compatibility_error
        end

        # Find suitable servers by topology type and operation type
        # Filter the suitable servers by calling the optional, application-provided server selector.
        # If there are any suitable servers, choose one at random from those within the latency window and return it;  otherwise, continue to the next step
        suitable_servers = find_suitable_servers(command, args, read_preference)
        selected_server = suitable_servers.try { |s| select_by_latency(s) }
        return selected_server if selected_server

        # Request an immediate topology check, then block the server selection thread until the topology changes or until the server selection timeout has elapsed
        @monitors.each { |monitor|
          monitor.request_immediate_scan
        }
        select
        when @topology_update.receive
        when timeout Time.utc - selection_timeout
        end

        # If more than serverSelectionTimeoutMS milliseconds have elapsed since the selection start time, raise a server selection error
        if Time.utc > selection_timeout
          raise ServerSelectionError.new "Timeout (#{@options.server_selection_timeout}) reached while trying to select a suitable server."
        end
      end
    end

    # def command(command cmd, ignore_errors = false, write_concern : WriteConcern? = @write_concern, read_concern : ReadConcern? = @read_concern, **args)
    #     super(cmd,  **args, ignore_errors: ignore_errors, write_concern: write_concern, read_concern: read_concern)
    # end

    def get_connection(server_description : SDAM::ServerDescription) : IO
      @pools[server_description] ||= DB::Pool(Mongo::Connection).new(
          initial_pool_size: @options.min_pool_size,
          max_pool_size: @options.max_pool_size,
          max_idle_pool_size: @options.min_pool_size
        ) do
          Mongo::Connection.new(server_description, @credentials, @options)
        end
      @pools[server_description].checkout.socket
    end

    def release_connection(server_description : SDAM::ServerDescription, socket : IO)
      @pools[server_description]?.try &.release(socket)
    end

    def close_connection_pool(server_description : SDAM::ServerDescription)
      @@lock.synchronize {
        pool = @pools.delete(server_description)
        pool.try &.close
      }
    end

    def stop_monitoring(server_description : SDAM::ServerDescription)
      @@lock.synchronize {
        @monitors.reject!(server_description)
      }
    end

    def on_topology_update
      loop do
        select
        when @topology_update.send nil
        else
          break
        end
      end

      @@lock.synchronize {
        self.topology.servers.each { |server|
          no_monitors = @monitors.none? { |monitor|
            monitor.server_description.address == server.address
          }
          if no_monitors
            monitor = SDAM::Monitor::Async.new(self, server, @credentials, @options.heartbeat_frequency || 10.seconds)
            @monitors << monitor
            monitor.scan
          end
        }
      }
    end

    def finalize
      @pools.each { |_, pool|
        pool.close
      }
      @monitors.each &.close
    end
  end
end
