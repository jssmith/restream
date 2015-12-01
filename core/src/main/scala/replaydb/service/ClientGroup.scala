package replaydb.service

import replaydb.service.driver._

class ClientGroup(runConfiguration: RunConfiguration) extends ClientGroupBase(runConfiguration) {
  override def getHandler() = new DriverServiceHandler(ClientGroup.this, runConfiguration)
}
