# ElasticSearch Reindexer

This is a tool that can be used to migrate data from an existing index to a new index with updated mappings.

The tool assumes that the index is behind an alias, and that the current index can be made read-only while the data is copied from the current index into a new version of that index. Once the new index has been written, the alias is cut over to the new version.

## Using the base Docker container
The `Dockerfile` in this project builds an intermediate container with an `ONBUILD` instruction, which will complete the build process when a child container uses this image in a `FROM` instruction. Such a project requires at least one git commit in its repository, and a file `mapping.json` in its root directory.

