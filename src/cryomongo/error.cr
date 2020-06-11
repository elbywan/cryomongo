module Mongo
  class Error < Exception
  end

  class HandshakeError < Error
  end

  class ConnectionError < Error
  end

  class ServerSelectionError < Error
  end

  class CommandError < Error

    # See: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering
    RECOVERING_CODES = {11600, 11602, 13436, 189, 91}
    RECOVERING_MESSAGES = {"not master or secondary", "node is recovering"}
    NOT_MASTER_CODES = {10107, 13435}
    NOT_MASTER_MESSAGES = {"not master"}
    SHUTDOWN_CODES   = {11600, 91}
    RESUMABLE_CODES  = {6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436, 63, 150, 13388, 234, 133}

    def initialize(code, message)
      @code = code.try &.as(Int32) || 0
      @message = message.try(&.as(String)) || ""
    end

    def to_s(io : IO)
      io << "Code: #{@code} - #{@message}"
    end

    def recovering?
      @code.in?(RECOVERING_CODES) ||
      RECOVERING_MESSAGES.any? &.in? @message.not_nil!
    end

    def not_master?
      @code.in?(NOT_MASTER_CODES) ||
      self.recovering? ||
      NOT_MASTER_MESSAGES.any? &.in? @message.not_nil!
    end

    def shutdown?
      @code.in? SHUTDOWN_CODES
    end

    def state_change?
      recovering? || not_master?
    end

    def resumable?
      @code.in? RESUMABLE_CODES
    end
  end
end
