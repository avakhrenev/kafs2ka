package avakhrenev.kafs2ka

import utest._

object BrokerStartTest extends TestSuite {
  val kafka = new RunningKafka(true)

  override val tests = this{

    'close {
      kafka.close()
    }
  }
}
