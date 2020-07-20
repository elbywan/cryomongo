module Mongo::Monitoring

  module Observable
    alias Subscription = Event -> Nil

    @observable_lock = Mutex.new
    @subscribers : Set(Subscription) = Set(Subscription).new

    def subscribe(&callback : Subscription) : Subscription
      @observable_lock.synchronize {
        @subscribers.add(callback)
      }
      callback
    end

    def unsubscribe(callback : Subscription) : Nil
      @observable_lock.synchronize {
        @subscribers.delete(callback)
      }
    end

    def broadcast(event : Event)
      @observable_lock.synchronize {
        @subscribers.each &.call(event)
      }
    end

    def has_subscribers?
      !@subscribers.empty?
    end
  end

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

  struct CommandStartedEvent < Event
    # Returns the command.
    getter command : BSON
    # Returns the database name.
    getter database_name : String

    def initialize(@command_name, @request_id, @address, @command, @database_name, @operation_id = nil)
    end
  end

  struct CommandSucceededEvent < Event
    # Returns the execution time of the event in the highest possible resolution for the platform.
    # The calculated value MUST be the time to send the message and receive the reply from the server
    # and MAY include BSON serialization and/or deserialization. The name can imply the units in which the
    # value is returned, i.e. durationMS, durationNanos.
    getter duration : Time::Span
    # Returns the command reply.
    getter reply : BSON

    def initialize(@command_name, @request_id, @address, @duration, @reply, @operation_id = nil)
    end
  end

  struct CommandFailedEvent < Event
    # Returns the execution time of the event in the highest possible resolution for the platform.
    # The calculated value MUST be the time to send the message and receive the reply from the server
    # and MAY include BSON serialization and/or deserialization. The name can imply the units in which the
    # value is returned, i.e. durationMS, durationNanos.
    getter duration : Time::Span
    # Returns the failure. Based on the language, this SHOULD be a message string, exception
    # object, or error document.
    getter failure : Exception
    # Returns the command reply.
    getter reply : BSON

    def initialize(@command_name, @request_id, @address, @duration, @failure, @reply, @operation_id = nil)
    end
  end
end
