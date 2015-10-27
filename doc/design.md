---
layout: default
title: Design
---

# Design Document

Goal Contributions

* High-speed access to historical data
* Enable “what if” scenario analysis
* Enable analysis “through time” rather than at points in time

Approach and Assumptions

* Parallelize replay of logs - want to put lots of parallel resources to work against a naturally serial problem statement
* Optimize computation for throughput rather than latency
* Memory is plentiful, but data is much larger still


Primitives

* Computation as DAG
  - We can compile our program to a directed acyclic graph of operations on events
* Map
  - Sorted and unsorted variants
  - Supports size operation
  - Random element selection (try to layer this on top of core API, perhaps using a prefix in the key)

Ideas to consider

* Garbage collection based on lifetime of object (in data time)