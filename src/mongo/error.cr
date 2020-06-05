module Mongo
  class Error < Exception
  end

  class CommandError < Error
    def initialize(code, message)
      @code = code.try &.as(Int32) || 0
      @message = message.try &.as(String) || ""
    end
  end

  class ServerSelectionError < Error
  end
end
