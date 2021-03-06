require "../spec_helper"

describe Mongo::URI do
  Dir.glob "./spec/connection-string/tests/*.json" do |file_path|
    tests = JSON.parse(File.open(file_path) { |file|
      file.gets_to_end
    })["tests"].as_a

    context "(#{file_path})" do
      # Crystal URI parser is more relaxed than the specs parser.
      next if file_path.ends_with? "invalid-uris.json"

      tests.each { |test|
        next if test["ignore"]?
        description = test["description"].as_s
        focus = test["focus"]?.try(&.as_bool) || false

        it "#{description}", focus: focus do
          uri = test["uri"].as_s
          if test["valid"]? == true
            seeds, mongo_options, credentials, db = Mongo::URI.parse(uri, Mongo::Options.new)
            # Hosts
            if hosts = test["hosts"].as_a?
              hosts.each { |host|
                address = host["host"].as_s
                if host["port"]? && host["port"].as_s?
                  address += ":#{host["port"].as_s}"
                end
                seeds.map(&.host).should contain address
              }
            end
            # Auth
            if auth = test["auth"].as_h?
              credentials.username.should eq auth["username"]?
              credentials.password.should eq auth["password"]?
              db.should eq (auth["db"]?.try(&.as_s?) || "")
              credentials.source.should eq (auth["db"]?.try(&.as_s?) || "")
            end
            # Options
            if options = test["options"].as_h?
              options.each { |option, value|
                opt = mongo_options.raw[option]?
                if value.raw.is_a? Number
                  opt.try(&.to_i32).should eq value
                else
                  opt.should eq value
                end
              }
            end
          else
            expect_raises(Exception) {
              Mongo::URI.parse(uri, Mongo::Options.new)
            }
          end
        end
      }
    end
  end
end
