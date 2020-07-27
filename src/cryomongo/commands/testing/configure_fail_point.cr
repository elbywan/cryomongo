require "bson"
require "../commands"

# To aid testing, MongoDB has the "configureFailpoint" command that can alter server behavior in a wide variety of ways, mostly simulating types of failure that are difficult to cause reliably in tests.
#
# To enable the "configureFailpoint" command, mongod must be started like:
# `mongod --setParameter enableTestCommands=1`
#
# NOTE: [for more details, please check the official MongoDB wiki](https://github.com/mongodb/mongo/wiki/The-%22failCommand%22-fail-point).
module Mongo::Commands::ConfigureFailPoint
  extend Command
  extend self

  # Returns a pair of OP_MSG body and sequences associated with the command and arguments.
  def command(fail_point : String, mode, options = nil)
    Commands.make({
      configureFailPoint: fail_point,
      mode:               mode,
      "$db":              "admin",
    }, options)
  end
end
