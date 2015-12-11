package replaydb.service

import replaydb.runtimedev.RuntimeStats
import replaydb.service.driver._

class ClientGroup(runConfiguration: RunConfiguration, stats: RuntimeStats) extends ClientGroupBase(runConfiguration, stats) {
  override def getHandler() = new DriverServiceHandler(ClientGroup.this, runConfiguration)
}
