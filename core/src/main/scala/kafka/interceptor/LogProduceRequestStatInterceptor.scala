package kafka.interceptor

import kafka.network.{Processor, RequestChannel}
import org.apache.kafka.common.network.KafkaChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

import java.nio.channels.SocketChannel
import scala.collection.mutable

class LogProduceRequestStatInterceptor(val processor: Processor, time: Time, flushIntervalMs: Long, newTestCheckIntervalMs: Long) extends IProcessorInterceptor {
  private val logger = LoggerFactory.getLogger("channel.requests")

  private class PollToPollStat {
    private var pollToPollSum: Long = 0L
    private var pollCount: Long = 0L
    private var prevPollTimeMs: Option[Long] = None

    def updateStat(newPollTimeMs: Long): Unit = {
      prevPollTimeMs.foreach(prev => {
        pollToPollSum += (newPollTimeMs - prev)
        pollCount += 1
      })
      prevPollTimeMs = Some(newPollTimeMs)
    }

    def getAvgPollToPoll: Double = if (pollCount == 0) 0.0 else pollToPollSum.toDouble / pollCount.toDouble
  }

  private case class ProduceRequestStatResult(requestCount: Long, avgPollToPollMs: Double) {}

  private class ProduceRequestStat(flushIntervalMs: Long) {
    private var lastCheckedTimeNsOption: Option[Long] = None
    private val requestCountMap = new mutable.HashMap[String, Long]().withDefaultValue(0L)
    private val pollToPollMap = new mutable.HashMap[String, PollToPollStat]()

    def recordRequest(clientId: String): Unit = requestCountMap(clientId) += 1
    def recordPollToPoll(clientId: String, currentTimeMs: Long): Unit = pollToPollMap.getOrElseUpdate(clientId, new PollToPollStat).updateStat(currentTimeMs)

    def flushStatIfNeeded(): Option[Map[String, ProduceRequestStatResult]] = {
      val curTimeNs = time.nanoseconds()
      if (lastCheckedTimeNsOption.isEmpty) lastCheckedTimeNsOption = Some(curTimeNs)
      val needFlush = lastCheckedTimeNsOption.exists(t => curTimeNs - t > flushIntervalMs * 1_000_000L)
      if (needFlush) Some(flushStat()) else None
    }

    private def flushStat(): Map[String, ProduceRequestStatResult] = {
      val result = requestCountMap.map { case (channelId, count) =>
        channelId -> ProduceRequestStatResult(
          count, pollToPollMap.get(channelId).map(_.getAvgPollToPoll).getOrElse(0.0)
        )
      }.toMap

      requestCountMap.clear()
      pollToPollMap.clear()
      lastCheckedTimeNsOption = Some(time.nanoseconds())
      result
    }
  }

  private val produceStat = new ProduceRequestStat(flushIntervalMs)
  private var lastConnectionTime: Option[Long] = None

  override def init(): Unit = {}

  override def beforeEveryProcessorCycle(processor: Processor): Unit = {
    produceStat.flushStatIfNeeded() match {
      case Some(stats) =>
        if (stats.isEmpty) return
        val logContent = stats.map { case (channelId, stat) =>
          s"${processor.id}: [$channelId]: ${stat.requestCount} ${stat.avgPollToPollMs}"
        }.mkString("\n")
        logger.info("\n" + logContent)
      case None =>
    }
  }

  override def beforeRegisterNewChannel(channel: SocketChannel, processor: Processor): Unit = {
    val curTimeNs = time.nanoseconds()
    if (lastConnectionTime.exists(t => (curTimeNs - t) < (newTestCheckIntervalMs * 1_000_000L))) return
    logger.info(s"new test")
    lastConnectionTime = Some(curTimeNs)
  }

  override def beforePutCompletedReceive(channel: KafkaChannel, currentTimeMs: Long): Unit = {
    produceStat.recordPollToPoll(channel.getLastClientId, currentTimeMs)
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
  }

  override def beforeProcessResponse(response: RequestChannel.Response): Unit = {
    if (response.request.header.apiKey != ApiKeys.PRODUCE) return
    produceStat.recordRequest(response.request.header.clientId())
  }

  override def afterProcessResponse(response: RequestChannel.Response): Unit = {}

  override def shutdown(): Unit = {}
}

class LogProduceRequestStatInterceptorBuilder(time: Time, flushIntervalMs: Long = 1000, newTestCheckIntervalMs: Long = 100_000) extends IProcessorInterceptorBuilder {
  override def build(processor: Processor): IProcessorInterceptor = new LogProduceRequestStatInterceptor(processor, time, flushIntervalMs, newTestCheckIntervalMs)
}
