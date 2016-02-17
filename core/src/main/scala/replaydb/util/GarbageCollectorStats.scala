package replaydb.util

import java.lang.management.ManagementFactory
import javax.management.openmbean.CompositeData
import javax.management.{Notification, NotificationListener, NotificationEmitter}

import com.sun.management.GarbageCollectionNotificationInfo

import scala.collection.JavaConversions._

class GarbageCollectorStats {
  private case class Stats(collectionCount: Long, collectionTimeMs: Long)
  private var initialStats: Map[String,Stats] = _

  private def getStats: Map[String,Stats] = {
    val gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans
    gcMxBeans.map(b => b.getName -> new Stats(b.getCollectionCount, b.getCollectionTime)).toMap
  }
  def reset(): Unit = {
    initialStats = getStats
  }
  def registerNotifications(logFn: String => Unit): Unit = {
    for (gcMxBean <- ManagementFactory.getGarbageCollectorMXBeans) {
      gcMxBean.asInstanceOf[NotificationEmitter].addNotificationListener(
        new NotificationListener {
          override def handleNotification(notification: Notification, handback: scala.Any): Unit = {
            val gcInfo = GarbageCollectionNotificationInfo.from(notification.getUserData.asInstanceOf[CompositeData])
            val gcName = gcInfo.getGcName.replace(" ","_")
            val gcAction = gcInfo.getGcAction.replace(" ","_")
            val gcCause = gcInfo.getGcCause.replace(" ","_")
            val gcStartTime = gcInfo.getGcInfo.getStartTime
            val gcEndTime = gcInfo.getGcInfo.getEndTime
            val currentTime = System.currentTimeMillis()
            logFn(s"$gcName $gcAction $gcCause $gcStartTime $gcEndTime $currentTime")
          }
        }, null, null)
    }
  }
  override def toString: String = {
    (for ((name,stats) <- getStats) yield {
      val prevStats = initialStats(name)
      val collectionCount = stats.collectionCount - prevStats.collectionCount
      val collectionTimeMs = stats.collectionTimeMs - prevStats.collectionTimeMs
      s"$name: $collectionCount collections $collectionTimeMs ms"
    }).mkString(",")
  }
}
