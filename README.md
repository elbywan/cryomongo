<div align="center">
	<img src="icon.svg" width="128" height="128" />
	<h1>cryomongo</h1>
  <h3>A MongoDB driver written in pure Crystal.</h3>
  <a href="https://travis-ci.org/elbywan/cryomongo"><img alt="travis-badge" src="https://travis-ci.org/elbywan/cryomongo.svg?branch=master"></a>
  <a href="https://github.com/elbywan/cryomongo/actions?query=branch%3Amaster+workflow%3ASpecs"><img alt="Build Status" src="https://github.com/elbywan/cryomongo/workflows/Specs/badge.svg?branch=master"></a>
  <a href="https://github.com/elbywan/cryomongo/tags"><img alt="GitHub tag (latest SemVer)" src="https://img.shields.io/github/v/tag/elbywan/cryomongo"></a>
  <a href="https://github.com/elbywan/cryomongo/blob/master/LICENSE"><img alt="GitHub" src="https://img.shields.io/github/license/elbywan/cryomongo"></a>
</div>

<hr/>

#### Cryomongo is a high-performance MongoDB driver written in pure Crystal. (i.e. no C dependencies needed.)

*Compatible with MongoDB 3.6+. Tested against: 4.2.*

**⚠️ BETA state.**

> If you are looking for a higher-level object-document mapper library, you might want to check out the [`moongoon`](https://github.com/elbywan/moongoon) shard.

## Installation

1. Add the dependency to your `shard.yml`:

```yaml
dependencies:
  cryomongo:
    github: elbywan/cryomongo
```

2. Run `shards install`

## Usage

### Minimal working example

```crystal
require "cryomongo"

# Create a Mongo client, using a standard mongodb connection string.
client = Mongo::Client.new # defaults to: "mongodb://localhost:27017"

# Get database and collection.
database = client["database_name"]
collection = database["collection_name"]

# Perform crud operations.
collection.insert_one({ one: 1 })
collection.replace_one({ one: 1 }, { two: 2 })
bson = collection.find_one({ two: 2 })
puts bson.not_nil!.["two"] # => 2
collection.delete_one({ two: 2 })
puts collection.count_documents # => 0
```

### Complex example with serialization

```crystal
require "cryomongo"

# We take advantage of the BSON serialization capabilities provided by the `bson.cr` shard.
record User,
  name : String,
  banned : Bool? = false,
  _id : BSON::ObjectId = BSON::ObjectId.new,
  creation_date : Time = Time.utc do
  include BSON::Serializable
  include JSON::Serializable
end

# Initialize Client, Database and Collection.
client = Mongo::Client.new
database = client["database"]
users = database["users"]

# We set majority read and write at the Database level.
database.read_concern = Mongo::ReadConcern.new(level: "majority")
database.write_concern = Mongo::WriteConcern.new(w: "majority")

# Drop and recreate the Collection to ensure that we read later only the documents we inserted in this example.
{ Mongo::Commands::Drop, Mongo::Commands::Create }.each do |command|
  database.command(command, name: "users")
rescue e : Mongo::Error::Command
  # ignore the server error, drop will fail if the collection has not been created before.
end

# Insert User structures that are automatically serialized to BSON.
users.insert_many({ "John", "Jane" }.map { |name|
  User.new(name: name)
}.to_a)

# Fetch a Cursor pointing to the users collection.
cursor = users.find

# Iterate the cursor and use `.of(User)` to deserialize as the cursor gets iterated.
# Then push the users into an array that gets pretty printed.
puts cursor.of(User).to_a.to_pretty_json
# => [
#   {
#     "name": "John",
#     "banned": false,
#     "_id": {
#       "$oid": "f2001c5fb0a33e0264e2ea05"
#     },
#     "creation_date": "2020-07-25T09:52:50Z"
#   },
#   {
#     "name": "Jane",
#     "banned": false,
#     "_id": {
#       "$oid": "f2001c5fb0a33e0264e2ea07"
#     },
#     "creation_date": "2020-07-25T09:52:50Z"
#   }
# ]
```

## Features

- **[CRUD operations](https://docs.mongodb.com/manual/crud/index.html)**
- **[Aggregation](https://docs.mongodb.com/manual/aggregation/) (except: Map-Reduce)**
- **[Bulk](https://docs.mongodb.com/manual/reference/method/Bulk/index.html)**
- **[Read](https://docs.mongodb.com/manual/reference/read-concern/index.html) and [Write](https://docs.mongodb.com/manual/reference/write-concern/) Concerns**
- **[Read Preference](https://docs.mongodb.com/manual/core/read-preference/index.html)**
- **[Authentication](https://docs.mongodb.com/manual/core/authentication/index.html) (only: SCRAM mechanisms)**
- **[TLS encryption](https://docs.mongodb.com/manual/core/security-transport-encryption/)**
- **[Indexes](https://docs.mongodb.com/manual/indexes/index.html)**
- **[GridFS](https://docs.mongodb.com/manual/core/gridfs/index.html)**
- **[Change Streams](https://docs.mongodb.com/manual/changeStreams/index.html)**
- **[Admin/Diagnostic commands](https://elbywan.github.io/cryomongo/Mongo/Commands.html)**
- **[Tailable and Awaitable cursors](https://docs.mongodb.com/manual/core/tailable-cursors/index.html)**
- **[Collation](https://docs.mongodb.com/manual/reference/collation/index.html)**
- **Standalone, [Sharded](https://docs.mongodb.com/manual/sharding/) or [ReplicaSet](https://docs.mongodb.com/manual/replication/) topologies**
- **[Command monitoring](https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst)**
- **Retryable [reads](https://docs.mongodb.com/manual/core/retryable-reads/) and [writes](https://docs.mongodb.com/manual/core/retryable-writes/)**
- **[Causal consistency](https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/#client-sessions-and-causal-consistency-guarantees)**
- **[Transactions](https://docs.mongodb.com/manual/core/transactions/)**

## Conventions

- Methods and arguments names are in **snake case**.
- Object arguments can usually be passed as a **[NamedTuple](https://crystal-lang.org/api/NamedTuple.html)**, **[Hash](https://crystal-lang.org/api/Hash.html)**, **[BSON::Serializable](https://github.com/elbywan/bson.cr#serialization)** or a **[BSON](https://elbywan.github.io/bson.cr/BSON.html)** instance.

## Documentation

**The generated API documentation is available [here](https://elbywan.github.io/cryomongo/Mongo.html).**

### Connection

```crystal
require "cryomongo"

# Mongo::Client is the root object for interacting with a MongoDB deployment.
# It is responsible for monitoring the cluster, routing the requests and managing the socket pools.

# A client can be instantiated using a standard mongodb connection string.

# Client options can be passed as query parameters…
client = Mongo::Client.new("mongodb://address:port/database?appname=MyApp")
# …or with a Mongo::Options instance…
options = Mongo::Options.new(appname: "MyApp")
client = Mongo::Client.new("mongodb://address:port/database", options)
# …or both.

# Instantiate objects to interact with a specific database or a collection…
database   = client["database_name"]
collection = database["collection_name"]
# …or using `default_database` if the connection uri string contains a default auth database component ("/database").
database   = client.default_database
collection = database.not_nil!.collection["collection_name"]

# The overwhelming majority of programs should use a single client and should not bother with closing clients.
# Otherwise, to free the underlying resources a client must be manually closed.
client.close
```

```crystal
# To enable SSL/TLS, use the `tls` option, alongside the `tlsCAFile` and `tlsCertificateKeyFile` options.
uri = "mongodb://localhost:27017/?tls=true&tlsCAFile=./ca.crt&tlsCertificateKeyFile=./client.pem"
ssl_client = Mongo::Client.new uri
```

**Links**

- [Mongo::Client](https://elbywan.github.io/cryomongo/Mongo/Client.html)
- [Mongo::Options](https://elbywan.github.io/cryomongo/Mongo/Options.html)

### Authentication

*Cryomongo only supports the SCRAM-SHA1 and SCRAM-SHA256 authentication methods without SASLprep.*

```crystal
require "cryomongo"

# To use authentication, specify a username and password when passing an URI to the client constructor.
# Authentication methods depend on the server configuration and on the value of the `authMechanism` query parameter.
client = Mongo::Client.new("mongodb://username:password@localhost:27017")
```

### Basic operations

```crystal
require "cryomongo"

client = Mongo::Client.new

# Most CRUD operations are performed at collection-level.
collection = client["database_name"]["collection_name"]

# The examples below are very basic, but the methods can accept all the options documented in the MongoDB manual.

## Create

# Insert a single document
collection.insert_one({ key: "value" })
# Insert multiple documents
collection.insert_many((1..100).map{|i| { count: i }}.to_a)

# To track the _id, generate and pass it as a property
id = BSON::ObjectId.new
collection.insert_one({ _id: id, key: "value" })

## Read

# Find a single document
document = collection.find_one({ _id: id })
document.try { |d| puts d.to_json }
# Find multiple documents.
cursor = collection.find({ qty: { "$gt": 4 }})
elements = cursor.to_a # cursor is an Iterable(BSON)

## Update

# Replace a single document.
collection.replace_one({ name: "John" }, { name: "Jane" })
# Update a single document.
collection.update_one({ name: "John" }, { "$set": { name: "Jane" }})
# Update multiple documents
collection.update_many({ name: { "$in": ["John", "Jane"] }}, { "$set": { name: "Jules" }})
# Find one document and replace it
document = collection.find_one_and_replace({ name: "John" }, { name: "Jane" })
puts document.try &.["name"]
# Find one document and update it
document = collection.find_one_and_update({ name: "John" }, { "$set": { name: "Jane" }})
puts document.try &.["name"]

## Delete

# Delete one document
collection.delete_one({ age: 20 })
# Delete multiple documents
collection.delete_many({ age: { "$lt": 18 }})
# find_one_and_delete
document = collection.find_one_and_delete({ age: { "$lt": 18 }})
puts document.try &.["age"]

# Aggregate

# Perform an aggregation pipeline query
cursor = collection.aggregate([
  {"$match": { status: "available" }}
  {"$limit": 5},
])
cursor.try &.each { |bson| puts bson.to_json }

# Distinct collection values
values = collection.distinct(
  key: "field",
  filter: { age: { "$gt": 18 }}
)

# Documents count
counter = collection.count_documents({ age: { "$lt": 18 }})

# Estimated count
counter = collection.estimated_document_count
```

**Links**

- [Mongo::Collection](https://elbywan.github.io/cryomongo/Mongo/Collection.html)
- [Mongo::Database](https://elbywan.github.io/cryomongo/Mongo/Database.html)

### Bulk operations

```crystal
require "cryomongo"

client = Mongo::Client.new

# A Bulk object can be initialized by calling `.bulk` on a collection.
collection = client["database_name"]["collection_name"]
bulk = collection.bulk
# A bulk is ordered by default.
bulk.ordered? # => true

500.times { |idx|
  # Build the queries by calling bulk methods multiple times.
  bulk.insert_one({number: idx})
  bulk.delete_many({number: {"$lt": 450}})
  bulk.replace_one({ number: idx }, { number: idx + 1})
}

# Execute all the queries and return an aggregated result.
pp bulk.execute(write_concern: Mongo::WriteConcern.new(w: 1))
```

**Links**

- [Mongo::Bulk](https://elbywan.github.io/cryomongo/Mongo/Bulk.html)

### Indexes

```crystal
require "cryomongo"

client = Mongo::Client.new
collection = client["database_name"]["collection_name"]

# Create one index without options…
collection.create_index(
  keys: {
    "a":  1,
    "b":  -1,
  }
)
# or with options (snake_cased)…
collection.create_index(
  keys: {
    "a":  1,
    "b":  -1,
  },
  options: {
    unique: true
  }
)
# and optionally specify the name.
collection.create_index(
  keys: {
    "a":  1,
    "b":  -1,
  },
  options: {
    name: "index_name",
  }
)

# Follow the same rules to create multiple indexes with a single method call.
collection.create_indexes([
  {
    keys: { a: 1 }
  },
  {
    keys: { b: 2 }, options: { expire_after_seconds: 3600 }
  }
])
```

**Links**

- [Mongo::Collection](https://elbywan.github.io/cryomongo/Mongo/Collection.html)

### GridFS

```crystal
require "cryomongo"

client = Mongo::Client.new
database = client["database_name"]

# A GridFS bucket belong to a database.
gridfs = database.grid_fs

# Upload
file = File.new("file.txt")
id = gridfs.upload_from_stream("file.txt", file)
file.close

# Download
stream = IO::Memory.new
gridfs.download_to_stream(id, stream)
puts stream.rewind.gets_to_end

# Find
files = gridfs.find({
  length: {"$gte": 5000},
})
files.each { |file|
  puts file.filename
}

# Delete
gridfs.delete(id)

# And many more methods… (check the link below.)
```

**Links**

- [Mongo::GridFS::Bucket](https://elbywan.github.io/cryomongo/Mongo/GridFS/Bucket.html)

### Change streams

```crystal
require "cryomongo"

# Change streams can watch a client, database or collection for change.
# This code snippet will focus on watching a single collection.

client = Mongo::Client.new
collection = client["database_name"]["collection_name"]

spawn {
  cursor = collection.watch(
    [
      {"$match": {"operationType": "insert"}},
    ],
    max_await_time_ms: 10000
  )
  # cursor.of(BSON) converts fetched elements to the Mongo::ChangeStream::Document(BSON) type.
  cursor.of(BSON).each { |doc|
    puts doc.document_key
    puts doc.full_document.to_json
  }
}

100.times do |i|
  collection.insert_one({count: i})
end

sleep
```

**Links**

- [Mongo::ChangeStream::Cursor](https://elbywan.github.io/cryomongo/Mongo/ChangeStream/Cursor.html)
- [Mongo::ChangeStream::Document](https://elbywan.github.io/cryomongo/Mongo/ChangeStream/Document.html)

## Raw commands

```crystal
require "cryomongo"

# Commands can be run on a client, database or collection depending on the command target.

client = Mongo::Client.new

# Call the `.command` method to run a command against the server.
# The first argument is a `Mongo::Commands` sub-class, followed by the mandatory arguments
# and finally an *options* named tuple containing the optional parameters in snake_case.
result = client.command(Mongo::Commands::ServerStatus, options: {
  repl: 0
})
puts result.to_bson

# The .command method can also be called against a Database…
client["database"].command(Mongo::Commands::Create, name: "collection")
client["database"].command(Mongo::Commands::Drop, name: "collection")
# …or a Collection.
client["database"]["collection"].command(Mongo::Commands::Validate)
```
**Links**

- [Mongo::Commands](https://elbywan.github.io/cryomongo/Mongo/Commands.html)
- [Mongo::Client#command](https://elbywan.github.io/cryomongo/Mongo/Client.html#command(command,write_concern:WriteConcern?=nil,read_concern:ReadConcern?=nil,read_preference:ReadPreference?=nil,server_description:SDAM::ServerDescription?=nil,session:Session::ClientSession?=nil,operation_id:Int64?=nil,**args)-instance-method)
- [Mongo::Database#command](https://elbywan.github.io/cryomongo/Mongo/Database.html#command(operation,write_concern:WriteConcern?=nil,read_concern:ReadConcern?=nil,read_preference:ReadPreference?=nil,session:Session::ClientSession?=nil,**args)-instance-method)
- [Mongo::Collection#command](https://elbywan.github.io/cryomongo/Mongo/Collection.html#command(operation,write_concern:WriteConcern?=nil,read_concern:ReadConcern?=nil,read_preference:ReadPreference?=nil,session:Session::ClientSession?=nil,**args)-instance-method)

## Concerns and Preference

```crystal
require "cryomongo"

# Instantiate Read/Write Concerns and Preference
read_concern = Mongo::ReadConcern.new(level: "majority")
write_concern = Mongo::WriteConcern.new(w: 1, j: true)
read_preference = Mongo::ReadPreference.new(mode: "primary")

# They can be set at the client, database or client level…
client = Mongo::Client.new
database = client["database_name"]
collection = database["collection_name"]

client.read_concern = read_concern
database.write_concern = write_concern
collection.read_preference = read_preference

# …or by passing an extra argument when calling a method.
collection.find(
  filter: { key: "value" },
  read_concern:  Mongo::ReadConcern.new(level: "local"),
  read_preference: Mongo::ReadPreference.new(mode: "secondary")
)
```

**Links**

- [Mongo::ReadConcern](https://elbywan.github.io/cryomongo/Mongo/ReadConcern.html)
- [Mongo::WriteConcern](https://elbywan.github.io/cryomongo/Mongo/WriteConcern.html)
- [Mongo::ReadPreference](https://elbywan.github.io/cryomongo/Mongo/ReadPreference.html)

## Commands Monitoring

```crystal
require "cryomongo"

client = Mongo::Client.new

# A simple logging subscriber.

subscription = client.subscribe_commands { |event|
  case event
  when Mongo::Monitoring::Commands::CommandStartedEvent
    Log.info { "COMMAND.#{event.command_name} #{event.address} STARTED: #{event.command.to_json}" }
  when Mongo::Monitoring::Commands::CommandSucceededEvent
    Log.info { "COMMAND.#{event.command_name} #{event.address} COMPLETED: #{event.reply.to_json} (#{event.duration}s)" }
  when Mongo::Monitoring::Commands::CommandFailedEvent
    Log.info { "COMMAND.#{event.command_name} #{event.address} FAILED: #{event.failure.inspect} (#{event.duration}s)" }
  end
}

# Make some queries…
client["database_name"]["collection_name"].find({ hello: "world" })

# …and eventually at some point, unsubscribe the logger.
client.unsubscribe_commands(subscription)
```

**Links**

- [Mongo::Client#subscribe_commands](https://elbywan.github.io/cryomongo/Mongo/Client.html#subscribe_commands(&callback:Monitoring::Commands::Event->Nil):Monitoring::Commands::Event->Nil-instance-method)
- [Mongo::Client#unsubscribe_commands](https://elbywan.github.io/cryomongo/Mongo/Client.html#unsubscribe_commands(callback:Monitoring::Commands::Event->Nil):Nil-instance-method)
- [Mongo::Monitoring::Observable](https://elbywan.github.io/cryomongo/Mongo/Monitoring/Observable.html)
- [Mongo::Monitoring::CommandStartedEvent](https://elbywan.github.io/cryomongo/Mongo/Monitoring/Commands/CommandStartedEvent.html)
- [Mongo::Monitoring::CommandSucceededEvent](https://elbywan.github.io/cryomongo/Mongo/Monitoring/Commands/CommandSucceededEvent.html)
- [Mongo::Monitoring::CommandFailedEvent](https://elbywan.github.io/cryomongo/Mongo/Monitoring/Commands/CommandFailedEvent.html)

## Causal Consistency

```crystal
require "cryomongo"

client = Mongo::Client.new
# It is important to ensure that both read and writes are performed with "majority" concern.
# See: https://docs.mongodb.com/manual/core/causal-consistency-read-write-concerns/
client.read_concern = Mongo::ReadConcern.new(level: "majority")
client.write_concern = Mongo::WriteConcern.new(w: "majority")

# Reusing the original Mongodb example.
# See: https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/#examples

current_date = Time.utc
items = client["test"]["items"]

# MongoDB enables causal consistency in client sessions by default.
# This is the block syntax that creates, ends and pass the session to collection methods automatically.
items_collection.with_session do |items|
  # Using a causally consistent session ensures that the update occurs before the insert.
  items.update_one(
    { sku: "111", end: { "$exists": false } },
    { "$set": { end: current_date }}
  )
  items.insert_one({ sku: "nuts-111", name: "Pecans", start: current_date })
  puts items.find.to_a.to_pretty_json
end

client.close
```

**Links**

- [Mongo::Session](https://elbywan.github.io/cryomongo/Mongo/Session.html)
- [Mongo::Client#start_session](https://elbywan.github.io/cryomongo/Mongo/Client.html#start_session(*,causal_consistency:Bool=true):Session::ClientSession-instance-method)
- [Mongo::Collection#with_session](https://elbywan.github.io/cryomongo/Mongo/Collection.html#with_session(**args,&)-instance-method)

## Transactions

```crystal
require "cryomongo"

# Initialize Client and Database instances.
client = Mongo::Client.new
database = client["db"]
collection = database["collection"]

# Create the collection.
{Mongo::Commands::Drop, Mongo::Commands::Create}.each do |command|
  database.command(command, name: "collection")
rescue e : Mongo::Error::Command
  # ignore the server error, drop will fail if the collection has not been created before.
end

# Set read and write concerns to perform isolated transactions.
# See: https://docs.mongodb.com/master/core/transactions/#transactions-and-sessions
transaction_options = Mongo::Session::TransactionOptions.new(
  read_concern: Mongo::ReadConcern.new(level: "snapshot"),
  write_concern: Mongo::WriteConcern.new(w: "majority")
)

# There are two ways to perform transactions:

collection.with_session(default_transaction_options: transaction_options) do |collection, session|
  puts collection.find.to_a.to_json # => "[]"

  # 1. by calling the `with_transaction` method.

  # `with_transaction` will commit after the block ends.
  # if the block raises, the transaction will be aborted.
  session.with_transaction {
    collection.insert_one({_id: 1})
    collection.insert_one({_id: 2})
  }
  puts collection.find.to_a.to_json # => [{"_id":1},{"_id":2}]

  # The transaction below will be aborted because the block raises an Exception.
  begin
    session.with_transaction {
      collection.insert_one({_id: 3})
      raise "Interrupted!"
      collection.insert_one({_id: 4})
    }
  rescue e
    puts e # => Interrupted!
  end
  puts collection.find.to_a.to_json # => [{"_id":1},{"_id":2}]

  # 2. by calling the `start_transaction`, `commit_transaction` and `abort_transaction` methods.
  session.start_transaction
  collection.insert_one({_id: 3})
  # The transaction is isolated, reading outside of the session scope does not return documents impacted by the transaction…
  puts database["collection"].find.to_a.to_json # => [{"_id":1},{"_id":2}]
  # but reading within the session scope does.
  puts collection.find.to_a.to_json # => [{"_id":1},{"_id":2},{"_id":3}]
  session.commit_transaction
  # The transaction is now committed and visible outside of the transaction scope.
  puts collection.find.to_a.to_json             # => [{"_id":1},{"_id":2},{"_id":3}]
  puts database["collection"].find.to_a.to_json # => [{"_id":1},{"_id":2},{"_id":3}]
end
```

**Links**

- [Mongo::Session#with_transaction](https://elbywan.github.io/cryomongo/Mongo/Session/ClientSession.html#with_transaction(**options,&)-instance-method)
- [Mongo::Session#start_transaction](https://elbywan.github.io/cryomongo/Mongo/Session/ClientSession.html#start_transaction(**options)-instance-method)
- [Mongo::Session#commit_transaction](https://elbywan.github.io/cryomongo/Mongo/Session/ClientSession.html#commit_transaction(*,write_concern:WriteConcern?=nil)-instance-method)
- [Mongo::Session#abort_transaction](https://elbywan.github.io/cryomongo/Mongo/Session/ClientSession.html#abort_transaction(*,write_concern:WriteConcern?=nil)-instance-method)
- [Mongo::Session::TransactionOptions](https://elbywan.github.io/cryomongo/Mongo/Session/TransactionOptions.html)

## Specifications

The goal is to to be compliant with most of the [official MongoDB set of specifications](https://github.com/mongodb/specifications).

**✅ Implemented**

 The following specifications are implemented:

- https://github.com/mongodb/specifications/tree/master/source/message
- https://github.com/mongodb/specifications/tree/master/source/crud
- https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
- https://github.com/mongodb/specifications/blob/master/source/driver-bulk-update.rst
- https://github.com/mongodb/specifications/blob/master/source/read-write-concern/read-write-concern.rst
- https://github.com/mongodb/specifications/blob/master/source/enumerate-collections.rst
- https://github.com/mongodb/specifications/blob/master/source/enumerate-databases.rst
- https://github.com/mongodb/specifications/blob/master/source/enumerate-indexes.rst
- https://github.com/mongodb/specifications/tree/master/source/connection-string
- https://github.com/mongodb/specifications/tree/master/source/uri-options (except validation)
- https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring
- https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst
- https://github.com/mongodb/specifications/blob/master/source/max-staleness/max-staleness.rst
- https://github.com/mongodb/specifications/tree/master/source/connection-monitoring-and-pooling (loosely - using the crystal-db pool)
- https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst (SHA1 / SHA256 only - without SASLprep)
- https://github.com/mongodb/specifications/blob/master/source/index-management.rst (no IndexView fluid syntax)
- https://github.com/mongodb/specifications/tree/master/source/gridfs
- https://github.com/mongodb/specifications/tree/master/source/change-streams
- https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst
- https://github.com/mongodb/specifications/tree/master/source/command-monitoring
- https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst
- https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst
- https://github.com/mongodb/specifications/tree/master/source/causal-consistency
- https://github.com/mongodb/specifications/blob/master/source/initial-dns-seedlist-discovery
- https://github.com/mongodb/specifications/tree/master/source/transactions

**⏳Next**

The following specifications are to be implemented next:

- https://github.com/mongodb/specifications/blob/master/source/polling-srv-records-for-mongos-discovery
- https://github.com/mongodb/specifications/tree/master/source/compression

## Contributing

1. Fork it (<https://github.com/elbywan/cryomongo/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [elbywan](https://github.com/elbywan) - creator and maintainer

## Credit

- Icon made by [Smashicons](https://www.flaticon.com/authors/smashicons) from [www.flaticon.com](https://www.flaticon.com).
