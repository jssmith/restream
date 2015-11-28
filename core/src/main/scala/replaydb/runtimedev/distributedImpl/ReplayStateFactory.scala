package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.{ReplayCounter, ReplayMap, ReplayTimestampLocalMap}

import scala.reflect.ClassTag

class ReplayStateFactory extends replaydb.runtimedev.ReplayStateFactory {
   val useParallel = true

   def getReplayMap[K, V : ClassTag](default: => V): ReplayMap[K, V] = {
     null
   }

   def getReplayCounter: ReplayCounter = {
     null
   }

   def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
     null
   }
 }
