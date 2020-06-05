require "./monitor"

require "../../commands/replication/is_master"
require "../../messages/message"

module Mongo::SDAM
  class Monitor::Global < Monitor
    @client : Mongo::Client::Single

    def initialize(@client : Mongo::Client::Single, @heartbeat_frequency  : Time::Span = 60.seconds)
      super(@client.topology, @heartbeat_frequency)
    end

    def get_connection(server_description : ServerDescription)
      @client.get_connection(server_description)
    end

    def close_connection(server_description : ServerDescription)
      @client.close_connection(server_description)
    end

    def scan
      # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#scanning
      before_cooldown = Time.utc - @cooldown

      loop do
        servers_to_check = @topology.servers.reject { |server|
          server.type.unknown? &&
          server.last_update_time > before_cooldown
        }
        break unless servers_to_check.size > 0

        server_to_check = servers_to_check[0]

        i = 0
        while i < servers_to_check.size
          server = servers_to_check[i]
          if server.try &.primary_or_possible?
            server_to_check = server
            break
          elsif !server.unknown_or_ghost?
            if server_to_check.unknown_or_ghost? || server_to_check.last_update_time > server.last_update_time
              server_to_check = server
            end
          elsif server_to_check.unknown_or_ghost? && server_to_check.last_update_time > server.last_update_time
            server_to_check = server
          end
          i += 1
        end

        new_description = check(server_to_check)
        new_description && @topology.update(server_to_check, new_description)
      end
    end
  end
end
