package kafka.interceptor

import kafka.network.{Processor, RequestChannel}
import kafka.utils.Logging
import org.apache.kafka.common.network.KafkaChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

import java.nio.channels.SocketChannel
import scala.collection.mutable

class LogProduceRequestStatInterceptor(val processor: Processor,
                                       time: Time,
                                       newTestCheckIntervalMs: Long) extends IProcessorInterceptor with Logging {

  private val channelRequestsLogger = LoggerFactory.getLogger("channel.requests")

  private val produceStat = new ProduceRequestStat()
  private var lastLoggingTimeNs: Option[Long] = None

  private var lastConnectionTimeNs: Option[Long] = None

  private val paddedProcessorInfo = padRight(s"processorId=${processor.id}(${if (processor.maybeRequestHandlerBuilder.isDefined) "D" else "N"})", 18)

  override def init(): Unit = {}

  override def beforeEveryProcessorCycle(): Unit = {
    val nowNs = time.nanoseconds()
    logProduceStatIfNeeded(nowNs)
  }

  override def beforeRegisterNewChannel(channel: SocketChannel): Unit = {
    if (checkOrUpdateLastConnectionTime()) {
      channelRequestsLogger.info("new test")
      info("new test")
    }
  }

  override def beforeReregisterNewChannel(channel: KafkaChannel): Unit = {
    checkOrUpdateLastConnectionTime()
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

  private def checkOrUpdateLastConnectionTime(): Boolean = {
    val nowNs = time.nanoseconds()
    if (lastConnectionTimeNs.exists(t => (nowNs - t) < newTestCheckIntervalMs * 1_000_000)) return false
    lastConnectionTimeNs = Some(nowNs)
    true
  }

  private def logProduceStatIfNeeded(nowNs: Long): Unit = {
    if (lastLoggingTimeNs.isEmpty) lastLoggingTimeNs = Some(nowNs)
    if (lastLoggingTimeNs.exists(t => nowNs - t < 1_000_000_000L)) return
    lastLoggingTimeNs = Some(nowNs)

    produceStat.consumeStats { (clientId, ops, avgPollToPollMs) =>
      val sb = new StringBuilder(128)
      sb.append(paddedProcessorInfo)
      appendPadLeft(sb, s"[${clientId.replace("_connector", "")}]  ", 22)
      appendPadRight(sb, s"ops=$ops", 11)
      sb.append(f"P2P=$avgPollToPollMs%.4f")
      channelRequestsLogger.info(sb.toString())
    }
    produceStat.reset()
  }

  private def appendPadLeft(sb: StringBuilder, value: String, width: Int): Unit = {
    var i = value.length
    while (i < width) {
      sb.append(' ')
      i += 1
    }
    sb.append(value)
  }

  private def appendPadRight(sb: StringBuilder, value: String, width: Int): Unit = {
    sb.append(value)
    var i = value.length
    while (i < width) {
      sb.append(' ')
      i += 1
    }
  }

  private def padRight(value: String, width: Int): String = {
    if (value.length >= width) value
    else value + (" " * (width - value.length))
  }

  private class ProduceRequestStat {
    private val opsMap = mutable.Map[String, OpsStat]()
    private val pollToPollMap = mutable.Map[String, PollToPollStat]()

    def recordRequest(clientId: String): Unit = {
      opsMap.getOrElseUpdate(clientId, new OpsStat()).updateStat()
    }

    def recordPollToPoll(clientId: String, currentTimeMs: Long): Unit = {
      pollToPollMap.getOrElseUpdate(clientId, new PollToPollStat()).updateStat(currentTimeMs)
    }

    def consumeStats(f: (String, Long, Double) => Unit): Unit = {
      opsMap.foreach { case (clientId, opsStat) =>
        val ops = opsStat.getOps
        val avgP2p = pollToPollMap.get(clientId).fold(0.0)(_.getAvgPollToPoll)
        f(clientId, ops, avgP2p)
      }
    }

    def reset(): Unit = {
      opsMap.filterInPlace { case (clientId, opsStat) =>
        if (opsStat.getOps == 0) {
          pollToPollMap.remove(clientId)
          false
        } else {
         opsStat.reset()
         pollToPollMap.get(clientId).foreach(_.reset())
         true
        }
      }
    }
  }

  private class OpsStat {
    private var ops: Long = 0L

    def getOps: Long = ops
    def updateStat(): Unit = ops += 1
    def reset(): Unit = ops = 0L
  }

  private class PollToPollStat {
    private var pollToPollSum: Long = 0L
    private var pollCount: Long = 0L
    private var prevPollTimeMs: Option[Long] = None

    def updateStat(newPollTimeMs: Long): Unit = {
      prevPollTimeMs.foreach { prev =>
        pollToPollSum += (newPollTimeMs - prev)
        pollCount += 1
      }
      prevPollTimeMs = Some(newPollTimeMs)
    }

    def getAvgPollToPoll: Double = if (pollCount == 0) 0.0 else pollToPollSum.toDouble / pollCount.toDouble

    def reset(): Unit = {
      pollToPollSum = 0L
      pollCount = 0L
      prevPollTimeMs = None
    }
  }
}

class LogProduceRequestStatInterceptorBuilder(time: Time, newTestCheckIntervalMs: Long = 100_000) extends IProcessorInterceptorBuilder {
  override def build(processor: Processor): IProcessorInterceptor = new LogProduceRequestStatInterceptor(processor, time, newTestCheckIntervalMs)
}
