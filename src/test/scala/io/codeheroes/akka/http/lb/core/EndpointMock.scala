package io.codeheroes.akka.http.lb.core

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.codeheroes.akka.http.lb.Endpoint

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

class EndpointMock(endpoint: Endpoint) {

  private implicit val system = ActorSystem()
  private implicit val mat = ActorMaterializer()
  private implicit val ec = system.dispatcher
  private val _processed = new AtomicInteger(0)

  private def handler(request: HttpRequest): Future[HttpResponse] = {
    _processed.incrementAndGet()
    Future {
      Thread.sleep(100)
      HttpResponse(entity = HttpEntity.CloseDelimited(ContentTypes.`application/json`, Source.single(ByteString("{}"))))
    }
  }

  private val binding = Await.result(Http().bindAndHandleAsync(handler, endpoint.host, endpoint.port), 5 seconds)

  def processed() = _processed.get()

  def unbind(): Unit = Await.result(system.terminate(), 5 seconds)

}
