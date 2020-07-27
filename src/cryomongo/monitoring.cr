# Provides runtime information about commands to any 3rd party APM library as well internal driver use, such as logging.
#
# ```
# client = Mongo::Client.new
#
# subscription = client.subscribe_commands { |event|
#   case event
#   when Mongo::Monitoring::Commands::CommandStartedEvent
#     Log.info { "COMMAND.#{event.command_name} #{event.address} STARTED: #{event.command.to_json}" }
#   when Mongo::Monitoring::Commands::CommandSucceededEvent
#     Log.info { "COMMAND.#{event.command_name} #{event.address} COMPLETED: #{event.reply.to_json} (#{event.duration}s)" }
#   when Mongo::Monitoring::Commands::CommandFailedEvent
#     Log.info { "COMMAND.#{event.command_name} #{event.address} FAILED: #{event.failure.inspect} (#{event.duration}s)" }
#   end
# }
#
# client.unsubscribe_commands(subscription)
# ```
module Mongo::Monitoring
  enum Type
    Commands
  end

  # Provides an observable interface for the `Mongo::Client`.
  class Observable(T)
    @observable_lock = Mutex.new
    @subscribers : Set(T -> Nil) = Set(T -> Nil).new

    def broadcast(event : T)
      @observable_lock.synchronize {
        @subscribers.each &.call(event)
      }
    end

    def subscribe(&callback : T -> Nil) : T -> Nil
      @observable_lock.synchronize {
        @subscribers.add(callback)
      }
      callback
    end

    def unsubscribe(callback : T -> Nil) : Nil
      @observable_lock.synchronize {
        @subscribers.delete(callback)
      }
    end

    def has_subscribers?
      !@subscribers.empty?
    end
  end

  module Commands
    # Contains common event fields.
    abstract struct Event
      macro inherited
        # Returns the command name.
        getter command_name : String
        # Returns the driver generated request id.
        getter request_id : Int64
        # Returns the driver generated operation id. This is used to link events together such
        # as bulk write operations. OPTIONAL.
        getter operation_id : Int64?
        # Returns the server address.
        getter address : String
      end
    end

    # This event is triggered before sending a command to the server.
    struct CommandStartedEvent < Event
      # Returns the command.
      getter command : BSON
      # Returns the database name.
      getter database_name : String

      # :nodoc:
      def initialize(@command_name, @request_id, @address, @command, @database_name, @operation_id = nil)
      end
    end

    # This event is triggered when a command is successfully acknowledged by the server.
    struct CommandSucceededEvent < Event
      # Returns the execution time of the event in the highest possible resolution for the platform.
      getter duration : Time::Span
      # Returns the command reply.
      getter reply : BSON

      # :nodoc:
      def initialize(@command_name, @request_id, @address, @duration, @reply, @operation_id = nil)
      end
    end

    # This event is triggered when a command is rejected by the server.
    struct CommandFailedEvent < Event
      # Returns the execution time of the event in the highest possible resolution for the platform.
      getter duration : Time::Span
      # Returns the failure.
      getter failure : Exception
      # Returns the command reply.
      getter reply : BSON

      # :nodoc:
      def initialize(@command_name, @request_id, @address, @duration, @failure, @reply, @operation_id = nil)
      end
    end
  end
end
