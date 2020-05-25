module Mongo
  class Error < Exception
    def initialize(code, message)
      @code = code.as(Int32)
      @message = message.as(String)
    end
  end
end
