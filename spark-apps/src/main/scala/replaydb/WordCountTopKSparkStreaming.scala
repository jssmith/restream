package replaydb

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import replaydb.event.MessageEvent

object WordCountTopKSparkStreaming {

  val K = 10
  val WindowSize = Seconds(30)
  val PrintInterval = Seconds(1)

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Streaming Testing")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryoserializer.buffer.max", "250m")
  conf.set("spark.streaming.backpressure.enabled", "true")

  def main(args: Array[String]): Unit = {

    if (args.length != 7) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar master-ip awsAccessKey awsSecretKey baseFilename numPartitions batchSizeMs totalEvents
          |  Example values:
          |    master-ip      = 171.41.41.31
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    batchSizeMs    = 1000
          |    totalEvents    = 5000000
        """.stripMargin)
      System.exit(1)
    }

    if (args(0) == "local") {
      conf.setMaster(s"local[${args(2)+1}]")
    } else {
      conf.setMaster(s"spark://${args(0)}:7077")
    }

    val baseFn = args(3)
    val numPartitions = args(4).toInt
    val batchSizeMs = args(5).toInt
    val totalEvents = args(6).toInt

    val ssc = new StreamingContext(conf, Milliseconds(batchSizeMs))
    val hadoopConf = ssc.sparkContext.hadoopConfiguration;
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", args(1))
    hadoopConf.set("fs.s3.awsSecretAccessKey", args(2))

    ssc.checkpoint("s3://spark-restream/checkpoint")

    val kryoReceivers = (0 until numPartitions).map(i => new KryoFileReceiver(s"$baseFn-$i"))
    val kryoStreams = kryoReceivers.map(ssc.receiverStream(_))
    val messageStream = ssc.union(kryoStreams).filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
    val wordStream = messageStream.flatMap(_.content.toLowerCase.split(" ").map(_ -> 1))

    val windowedTopK = wordStream.reduceByKeyAndWindow(_ + _, _ - _, WindowSize)

    var lastPrintTime = Time(System.currentTimeMillis())
    windowedTopK.foreachRDD((rdd, time) => {
      if (time - lastPrintTime >= PrintInterval) {
        lastPrintTime = time
        val currentTopString = rdd.takeOrdered(K)(new Ordering[(String, Int)] {
          def compare(x: (String, Int), y: (String, Int)) = -1 * x._2.compare(y._2)
        }).mkString(", ")
//        println(s"current top $K: $currentTopString")
      }
    })

    val eventsProcessed = new AtomicLong
    wordStream.foreachRDD(rdd => { eventsProcessed.addAndGet(rdd.count()) })

    ssc.start()
    val startTime = System.currentTimeMillis()
    new Thread("Closer") {
      override def run(): Unit = {
        while (eventsProcessed.get() < totalEvents) {
          Thread.sleep(1000)
        }
        val processTime = System.currentTimeMillis() - startTime
        println(s"WordCountTopKSparkStreaming: $numPartitions partitions, $totalEvents events in $processTime ms (${totalEvents/(processTime/1000)} events/sec)")
        println(s"CSV,WordCountTopKSparkStreaming,$numPartitions,$totalEvents,$processTime,$batchSizeMs")
        ssc.stop(true)
      }
    }.start()
    ssc.awaitTermination()
  }
}
