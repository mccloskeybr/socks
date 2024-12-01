# Socks DB

A small relational database engine written in rust, primarily for education purposes.

Current feature set (12/1/2024):
- Support for a primary index only.
- Support retrieval of rows given a full key match.
- B-tree file format.

Planned features:
- LRU cache.
- SQL-like generalizable queries.
- Concurrent request processing.
- SIMD powered searches.
- Persistent access via. sockets.
- Transactions.

References:
- http://www.cs.columbia.edu/~kar/pubsk/simd.pdf
