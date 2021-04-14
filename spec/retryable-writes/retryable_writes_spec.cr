require "../spec_helper"

describe Mongo::Session do
  with_mongo { |get_client, topology, uri|
    Mongo::Spec::Runner.run_tests("./spec/retryable-writes/tests/*.json", get_client, topology, uri: uri)
  }
end
