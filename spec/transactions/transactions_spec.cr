require "../spec_helper"

describe Mongo::Session do
  with_mongo { |get_client, topology, uri|
    Mongo::Spec::Runner.run_tests("./spec/transactions/tests/*.json", get_client, topology, uri: uri, ignore_cursor_42s: true, distinct_workaround: true)
  }
end
