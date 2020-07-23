module Mongo
  class Error < Exception
  end

  # class Error::Handshake < Error
  # end

  # class Error::Connection < Error
  # end

  # Is raised during server selection when encountering a timeout.
  class Error::ServerSelection < Error
  end

  # Is raised when the server replies with an error to a command request.
  class Error::Command < Error
    getter code : Int32

    # See: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering
    RECOVERING_CODES    = {11600, 11602, 13436, 189, 91}
    RECOVERING_MESSAGES = {"not master or secondary", "node is recovering"}
    NOT_MASTER_CODES    = {10107, 13435}
    NOT_MASTER_MESSAGES = {"not master"}
    SHUTDOWN_CODES      = {11600, 91}
    # See: https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#determining-retryable-errors
    RETRYABLE_CODES     = {6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436}
    # See: https://github.com/mongodb/specifications/blob/f1fcb6aa9751e5ed7eb8e64c0f08f1edf10a859a/source/change-streams/change-streams.rst#resumable-error
    RESUMABLE_CODES     = {63, 133, 150, 234, 13388, 133} + RETRYABLE_CODES

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

    def retryable?
      @code.in? RETRYABLE_CODES
    end

    def resumable?
      @code.in? RESUMABLE_CODES
    end
  end

  # Is raised when the server replies to a write with one or more WriteErrors.
  class Error::CommandWrite < Error
    getter errors = [] of Error::Command

    def initialize(errors : BSON)
      errors.each { |_, error|
        error = error.as(BSON)
        err_msg = error["errmsg"]?.as(String)
        err_code = error["code"]?
        @errors << Error::Command.new(err_code, err_msg)
      }
    end

    def message
      @errors.join('\n')
    end
  end

  # Is raised when the server replies to a write with a WriteConcernError.
  class Error::WriteConcern < Error::Command
    getter details : BSON?

    def initialize(error : BSON)
      @code = error["code"]?.try &.as(Int32) || 0
      @message = error["errmsg"]?.try(&.as(String)) || ""
      @details = error["err_info"]?.try &.as(BSON)
    end
  end
end
