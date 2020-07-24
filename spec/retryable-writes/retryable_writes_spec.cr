require "../spec_helper"

describe Mongo::Session do
  with_mongo { |get_client, topology|
    Runner.run_tests("./spec/retryable-writes/tests/*.json", get_client, topology)
  }
end
