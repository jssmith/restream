package replaydb.io

import replaydb.event.{NewFriendshipEvent, MessageEvent}

class SocialNetworkStorage extends KryoEventStorage {
  kryo.register(classOf[MessageEvent])
  kryo.register(classOf[NewFriendshipEvent])
}
