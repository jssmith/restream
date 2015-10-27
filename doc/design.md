---
layout: default
title: Design
---

# Design Document #

## Goal Contributions ##

* High-speed access to historical data
* Enable “what if” scenario analysis
* Enable analysis “through time” rather than at points in time


## Approach ##

* Parallelize replay of logs - want to put lots of parallel resources to work against a naturally serial problem statement
* Optimize computation for throughput rather than latency


## Assumptions ##

* Memory is plentiful, but data set is much larger still
* Streaming access to history, approximately ordered (bound on difference out-of-order timestamps)
  - May make multiple passes over the data, may make such passes concurrently
  - Efficient sharded access to data (partition by event type, by hash)


## System Primitives ##

* Data structures understand time - motivation is that executing code can ask for state of the data at any previous point in time
* Maps for state
  - `put(K, V, T_begin, T_end)` - puts a key for range of valid timestamps
  - `get(K, T)` - get value of key at time `T`
  - `get(K_begin, K_end, T)` - range of values at time `T`
  - `size(T)` - size of map at time `T`
* Algebraic value types
  - We allow for structured values
  - Generic interface as `x.apply(op, y, T)` - (apply `op` with argument `y` to `x` at time `T`)
  - If operators commute everything is easy. If not, the program must ensure that they are called in temporal order.
  - Simple examples: counters, boolean (exists), etc.
  - More complex examples: lists, windowed lists, maps
* Computation as DAG
  - We can compile our program to a directed acyclic graph of operations on events
  - The runtime ensures that code that could update an element of state `X` for time `T` runs ahead of any code that could read state for element `X` at time `T`. A static analysis suffices when `X` is defined at the granularity of top-level data structures. We believe that when the program is acyclic this analysis provides sufficient granularity to extract a great deal of parallelism. An alternative analysis might “look ahead” in the incoming data to determine potential conflicts and to schedule execution accordingly.
* Open issues
  - We require some way of choosing an element at random from a map. Need to figure out whether this should be in the primitives, or layered on top.
  - Might provide garbage collection automatically, based on timestamps rather than reachability


## Language ##

- We have a bunch of English-language [query examples](example_queries_english.html) and are working on formal syntax alternatives.
- Most likely we will build a Scala DSL of some sort.

