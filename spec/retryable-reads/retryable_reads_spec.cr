require "../spec_helper"

describe Mongo::Session do
  with_mongo { |get_client, topology|
    Mongo::Spec::Runner.run_tests("./spec/retryable-reads/tests/*.json", get_client, topology)
  }
end