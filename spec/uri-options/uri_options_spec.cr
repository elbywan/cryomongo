require "../spec_helper"

describe Mongo::Options do
  Dir.glob "./spec/uri-options/tests/*.json" do |file_path|
    tests = JSON.parse(File.open(file_path) { |file|
      file.gets_to_end
    })["tests"].as_a

    context "(#{file_path})" do
      tests.each { |test|
        next if test["ignore"]?
        description = test["description"].as_s
        focus = test["focus"]?.try(&.as_bool) || false

        it "#{description}", focus: focus do
          uri = test["uri"].as_s
          if test["valid"]? == true
            _, mongo_options = Mongo::URI.parse(uri)
            # Options
            if options = test["options"].as_h?
              options.each { |option, value|
                option = option.downcase
                opt = mongo_options.raw[option]?
                if option == "readpreferencetags"
                  tag_array = value.as_a
                  mongo_options.read_preference_tags.each_with_index { |tag, idx|
                    strs = [] of String
                    tag_array[idx].as_h.each { |k, v|
                      strs << "#{k}:#{v.as_s}"
                    }
                    tag.should eq strs.join(",")
                  }
                elsif value.raw.is_a? Number
                  opt.try(&.to_i32).should eq value
                elsif value.raw.is_a? Bool
                  opt.try(&.== "true").should eq value
                elsif value.raw.is_a? Array
                  opt.should eq value.as_a.join(",")
                elsif value.raw.is_a? Hash
                  str = [] of String
                  value.as_h.each { |k, v|
                    str << "#{k}:#{v}"
                  }
                  opt.should eq str.join(",")
                else
                  opt.should eq value
                end
              }
            end
          else
            expect_raises(Exception) {
              Mongo::URI.parse(uri)
            }
          end
        end
      }
    end
  end
end
