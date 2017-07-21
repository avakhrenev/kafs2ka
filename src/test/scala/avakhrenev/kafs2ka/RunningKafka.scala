package avakhrenev.kafs2ka

import java.io.{Closeable, File}
import java.net.InetSocketAddress
import java.nio.file.{Files, Path}
import java.util.Properties
import java.util.concurrent.CopyOnWriteArrayList

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/** Starts in-process zookeeper and kafka broker */
class RunningKafka(autoCreateTopics: Boolean) extends Closeable {
  import RunningKafka._
  val kafkaPort            = getFreePort()
  val zooKeeperPort        = getFreePort()
  val zookeeper            = s"localhost:$zooKeeperPort"
  val kafkaBootstrap       = s"localhost:$kafkaPort"
  val kafkaLogsBase        = createTmpDir("kafka-logs")
  val zkLogsDir            = createTmpDir("zookeeper-logs")
  private val kafkaBrokers = new CopyOnWriteArrayList[(Int, Option[KafkaServer])]()

  val zkInstance = startZookeeper(zkLogsDir)
  addKafkaBroker(kafkaPort)

  private def startZookeeper(zkLogsDir: File, tickTime: Int = 2000) = {
    val zkServer = new ZooKeeperServer(zkLogsDir, zkLogsDir, tickTime)
    val factory  = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("0.0.0.0", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  /** Starts another kafka broker, returning its id */
  def addKafkaBroker(port: Int): Int = {
    val nextId = kafkaBrokers.size
    kafkaBrokers.add(port -> None)
    startBroker(nextId)
    nextId
  }

  /** Starts broker with a given ID if it has not been started yet */
  def startBroker(id: Int) = {
    val logsDir      = new File(kafkaLogsBase, id.toString)
    val (port, prev) = kafkaBrokers.get(id)
    if (prev.isEmpty) {
      val p: Properties = new Properties
      p.setProperty("zookeeper.connect", zookeeper)
      p.setProperty("broker.id", id.toString)
      p.setProperty("listeners", s"PLAINTEXT://0.0.0.0:$port")
      p.setProperty("advertised.listeners", s"PLAINTEXT://localhost:$port")
      p.setProperty("auto.create.topics.enable", autoCreateTopics.toString)
      p.setProperty("log.dir", logsDir.getAbsolutePath)
      p.setProperty("log.flush.interval.messages", 1.toString)
      p.setProperty("log.cleaner.dedupe.buffer.size", "1048577")

      val broker = new KafkaServer(new KafkaConfig(p))
      kafkaBrokers.set(id, port -> Some(broker))
      broker.startup()
    }
  }
  /** Stops broker with a given ID if it has not stopped yet */
  def stopBroker(id: Int) = {
    val (port, broker) = kafkaBrokers.get(id)
    kafkaBrokers.set(id, port -> None)
    broker.foreach { b =>
      b.shutdown(); b.awaitShutdown()
    }
  }


  override def close() = {
    val brokers = kafkaBrokers.asScala.flatMap(_._2)
    val failures =
      (brokers.map(t => Try(t.shutdown())) ++ brokers.map(t => Try(t.awaitShutdown()))).collect {
        case Failure(e) => e
      }
    zkInstance.shutdown()
    failures.foreach(t => throw t)
  }
}

object RunningKafka {
  def baseTmpDir = new File(sys.props("sbt.test.tempdir"))

  def createTmpDir(dirPrefix: String, basedir: File = baseTmpDir): File = {
    val dir: Path = basedir.toPath.resolve(dirPrefix)
    Files.createDirectories(dir.getParent)
    val result: Path = Files.createTempDirectory(dir.getParent, dir.getFileName.toString)
    Files.createDirectories(result)
    result.toFile
  }

  import java.net.ServerSocket
  def getFreePort() = {
    val ss = new ServerSocket(0)
    try {
      ss.getLocalPort
    } finally {
      ss.close()
    }
  }
}
