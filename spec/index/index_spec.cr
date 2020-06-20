require "../spec_helper"

describe Mongo::Index::Model do
  client = Mongo::Client.new
  collection = client["cryomongo"]["test"]

  before_all {
    `rm -Rf ./data`
    start_mongo
  }

  after_all {
    client.close
    stop_mongo
    sleep 1
    `rm -Rf ./data`
  }

  after_each {
    collection.drop_indexes
  }

  it "should create an index and drop it" do
    index_name = "_id_1_name_-1"

    collection.create_index(
      keys: {
        "_id":  1,
        "name": -1,
      }
    )

    indexes = collection.list_indexes.to_a
    indexes.size.should eq 2
    indexes[1]["name"].should eq index_name

    collection.drop_index(name: index_name)

    indexes = collection.list_indexes.to_a
    indexes.size.should eq 1
    indexes[0]["name"].should eq "_id_"
  end

  it "should create an index with a custom name" do
    index_name = "index_name"

    collection.create_index(
      keys: {
        "name": -1,
      },
      options: {
        name: "index_name",
      }
    )

    indexes = collection.list_indexes.to_a
    indexes.size.should eq 2
    indexes[1]["name"].should eq index_name
  end

  it "should create multiple indexes" do
    collection.create_indexes(
      models: [
        {
          keys: {
            "one": 1,
          },
          options: {
            name: "one",
          },
        },
        {
          keys: {
            "two": 1,
          },
          options: {
            name: "two",
          },
        },
      ]
    )

    indexes = collection.list_indexes.to_a
    indexes.size.should eq 3
    indexes[0]["name"].should eq "_id_"
    indexes[1]["name"].should eq "one"
    indexes[2]["name"].should eq "two"
  end
end
