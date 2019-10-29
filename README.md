# iris
A commit log service to handle large amount of small event data coming at very high speed


## Context
There are some application use cases where data is generated continuously. A web-based WYSIWYG editor is one such use case. They produce a type of ever-growing essentially infinite data set known as unbounded data. These editors might send delta events, that represent incremental operations and data. This stream of continuous event data would need to be handled sequentially and incrementally and then aggregated and stored.

Project iris stores a stream of events, without being too complex. It is a simple system that is suitable for small projects. It keeps a record of operations in the form of an append-only, totally-ordered sequence of records ordered by time. Records are appended to the end of the log, and each entry is assigned a unique sequential log entry number. 

The project is implemented in Go which has great support for both protocol buffers and gRPC. The advantage of using gRPC is its ability to break free from the call-and-response architecture necessitated by HTTP + JSON. gRPC is built on top of HTTP/2, which allows for client-side (and/or server-side) streaming. In iris, client-side streaming is used to easily support bulk ingestion.

## Objectives
- Implement a commit log service in Go
- Ingest event stream data using gRPC and Protocol Buffer
- Collect and move log data to a data store

