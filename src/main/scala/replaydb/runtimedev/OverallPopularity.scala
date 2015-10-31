package replaydb.runtimedev

import replaydb.event.Event

// define the data classes
case class ProductView(sku: Long, ts: Long) extends Event
case class ProductUpdate(sku: Long, ts: Long) extends Event

class OverallPopularity extends ReplayRunnable {
  val totalViewCt: ReplayCounter = null
  val totalUpdateCt: ReplayCounter = null
  val productViewCt: ReplayMap[Long, ReplayCounter] = null
  val allProducts: ReplayMap[Long,Int] = null

  def updateA(pu: ProductUpdate): Unit = {
    allProducts.put(pu.sku, 0, pu.ts)
  }

  def updateB(pv: ProductView): Unit = {
    totalViewCt.add(1, pv.ts)
    // TODO have pv.ts in here 2x
    productViewCt.update(pv.sku,_.add(1, pv.ts), pv.ts)
  }

  def updateC(pv: ProductView): Unit = {
    val productCt: Long = productViewCt.get(pv.sku, pv.ts).get.get(pv.ts)
    val otherProductCt = productViewCt.get(allProducts.getRandom(pv.ts).get, pv.ts).get.get(pv.ts)
    val allCt = totalViewCt.get(pv.ts)
    printf("1,%d,%d", productCt, allCt)
    printf("0,%d,%d", otherProductCt, allCt)
  }

}
