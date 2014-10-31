pubsub
======

A pubsub server for streams that guarantees sequence, allows a pyramid of servers to broadcast the stream to unlimited clients, and is designed for scale and performance. It is written in pure C and has a asynchronous architecture (epoll) to maximize throughput.
