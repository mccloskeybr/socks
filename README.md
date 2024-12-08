# Socks DB

A small relational database engine written in rust, primarily for education purposes. Not winning any awards, but ¯\\_(ツ)_/¯.

Current feature set (12/8/2024):
- Support for a primary index only.
- Support retrieval of rows given a full key match.
- B+ tree file format.
- SIMD accelerated reads / writes.

Limitations:
- No generic query language supported (yet).
- Keys can only be comprised of a single part.

Planned features:
- LRU cache.
- Concurrent request processing.
- Benchmarking suite.
- Transactions.
- SQL-like generalizable queries.
- Persistent access via. sockets.

Optimizations:
- Instead of using protos, read & write raw data.
- More efficient B+ tree balancing scheme.

References:
- http://www.cs.columbia.edu/~kar/pubsk/simd.pdf
- https://www.geeksforgeeks.org/introduction-of-b-tree/
