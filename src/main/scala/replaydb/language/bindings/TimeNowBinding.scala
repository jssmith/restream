package replaydb.language.bindings

class TimeNowBinding extends NamedTimeIntervalBinding("NOW", null, null, null) {

  override def toString: String = {
    "NOW"
  }

}
