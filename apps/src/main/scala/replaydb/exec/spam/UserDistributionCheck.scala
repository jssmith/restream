package replaydb.exec.spam

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

import scala.collection.mutable.ArrayBuffer

object UserDistributionCheck extends App {
  if (args.length > 2) {
    println("Usage: UserDistributionCheck filename [ limit ]")
    System.exit(1)
  }
  val fn = args(0)
  val limit = if (args.length == 2) {
    Integer.parseInt(args(1))
  } else {
    100
  }
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  case class UserPair(userIdA: Long, userIdB: Long)
  val messages = ArrayBuffer[UserPair]()
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream(fn)), e => {
    e match {
      case nfe: NewFriendshipEvent =>
      case me: MessageEvent =>
        messages += UserPair(me.senderUserId, me.recipientUserId)
    }
    pm.increment()
  }, limit)
  pm.finished()

  def countDistinct(x: Iterable[Long]) = {
    // result as (number of appearances, number of users)
    x.groupBy(y=>y).mapValues(_.size).groupBy(_._2).mapValues(_.size).toArray.sortBy(y=>y._1)
  }

  countDistinct(messages.map(_.userIdA)).foreach(println)
}
