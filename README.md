# mongo-crystal-driver

### A pure Crystal MongoDB driver.

### DO NOT USE, work in progress!

![Working…](https://media.giphy.com/media/o0vwzuFwCGAFO/giphy.gif)

**Implemented**

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
- https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring ("single-threaded" monitor/client for now, will be rewritten to async)
- https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst
- https://github.com/mongodb/specifications/blob/master/source/max-staleness/max-staleness.rst

**In the pipe**

- https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst

- https://github.com/mongodb/specifications/blob/master/source/index-management.rst
- https://github.com/mongodb/specifications/tree/master/source/connection-monitoring-and-pooling
- https://github.com/mongodb/specifications/tree/master/source/command-monitoring
- https://github.com/mongodb/specifications/tree/master/source/gridfs
- https://github.com/mongodb/specifications/tree/master/source/change-streams
- https://github.com/mongodb/specifications/tree/master/source/compression
- https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst
- https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     mongo:
       github: elbywan/mongo-crystal-driver
   ```

2. Run `shards install`

## Usage

```crystal
require "mongo"

client = Mongo::Client.new("mongodb://localhost:27017")
database = client["database_name"]
collection = database["collection_name"]

collection.insert_one({ one: 1 })
collection.replace_one({ one: 1 }, { two: 2 })
bson = collection.find_one({ two: 2 })
puts bson.not_nil!.["two"] # => 2
collection.delete_one({ two: 2 })
puts collection.count_documents # => 0
```

TODO: Write usage instructions here

## Development

TODO: Write development instructions here

## Contributing

1. Fork it (<https://github.com/your-github-user/mongo-crystal-driver/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [elbywan](https://github.com/your-github-user) - creator and maintainer
