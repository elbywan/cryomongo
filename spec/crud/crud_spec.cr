require "../spec_helper"

describe Mongo do
  with_mongo { |get_client, topology, uri|
    %w(
      v1
      v2
    ).each do |version|
      context "[#{version}]" do
        Mongo::Spec::Runner.run_tests("./spec/crud/tests/#{version}/**/*.json", get_client, topology, uri: uri)
      end
    end
  }
end
