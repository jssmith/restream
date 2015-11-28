package replaydb.service.driver

case class UpdateAllProgressCommand (progressMarks: Map[Int, Long]) extends Command
