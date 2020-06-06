require "./scram"

module Mongo::Auth
  enum Mechanism
    ScramSha1
    ScramSha256
    MongodbX509
    MongodbCR
    MongodbAWS
    GssApi
    Plain
  end
end
