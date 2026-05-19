package kafka.interceptor

import kafka.network.{Processor, RequestChannel}
import org.apache.kafka.common.network.{ISelectorInterceptor, ISelectorInterceptors, KafkaChannel}

import java.nio.channels.SocketChannel

trait IProcessorInterceptorBuilder {
  def build(processor: Processor): IProcessorInterceptor
}

trait IProcessorInterceptor extends ISelectorInterceptor {
  def init(): Unit

  def beforeEveryProcessorCycle(processor: Processor): Unit

  def beforeRegisterNewChannel(channel: SocketChannel, processor: Processor): Unit

  def beforeSendRequestToQueue(request: RequestChannel.Request): Unit

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit

  def afterProcessResponse(response: RequestChannel.Response): Unit

  def beforeProcessResponse(response: RequestChannel.Response): Unit

  def shutdown(): Unit
}

class ProcessorInterceptors(val interceptors: Vector[IProcessorInterceptor]) extends ISelectorInterceptors {

  def init(): Unit = {
    interceptors.foreach(_.init())
  }

  def beforeEveryProcessorCycle(processor: Processor): Unit = {
    interceptors.foreach(_.beforeEveryProcessorCycle(processor))
  }

  def beforeRegisterNewChannel(channel: SocketChannel, processor: Processor): Unit = {
    interceptors.foreach(_.beforeRegisterNewChannel(channel, processor))
  }

  override def beforePutCompletedReceive(channel: KafkaChannel, currentTimeMs: Long): Unit = {
    interceptors.foreach(_.beforePutCompletedReceive(channel, currentTimeMs))
  }

  def beforeSendRequestToQueue(request: RequestChannel.Request): Unit = {
    interceptors.foreach(_.beforeSendRequestToQueue(request))
  }

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    interceptors.foreach(_.beforeSendResponseToQueue(response))
  }

  def beforeProcessResponse(response: RequestChannel.Response): Unit = {
    interceptors.foreach(_.beforeProcessResponse(response))
  }

  def afterProcessResponse(response: RequestChannel.Response): Unit = {
    interceptors.foreach(_.afterProcessResponse(response))
  }

  def shutdown(): Unit = {
    interceptors.foreach(_.shutdown())
  }
}
