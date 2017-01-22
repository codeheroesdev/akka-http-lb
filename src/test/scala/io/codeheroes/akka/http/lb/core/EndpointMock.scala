package io.codeheroes.akka.http.lb.core

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import io.codeheroes.akka.http.lb.Endpoint

import scala.concurrent.Await
import scala.concurrent.duration._

class EndpointMock(endpoint: Endpoint) {

  private implicit val system = ActorSystem()
  private implicit val mat = ActorMaterializer()
  private val _processed = new AtomicInteger(0)

  private def handler(request: HttpRequest): HttpResponse = {
    _processed.incrementAndGet()
    HttpResponse()
  }

  private val binding = Await.result(Http().bindAndHandleSync(handler, endpoint.host, endpoint.port), 5 seconds)

  def processed() = _processed.get()

  def unbind(): Unit = Await.result(system.terminate(), 5 seconds)

}
