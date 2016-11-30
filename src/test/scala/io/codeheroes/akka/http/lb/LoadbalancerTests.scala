package io.codeheroes.akka.http.lb

import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import io.codeheroes.akka.http.lb.core.{TestLatch, EndpointMock}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.{Duration, _}

class LoadbalancerTests extends FlatSpec with Matchers {
  private implicit val system = ActorSystem()
  private implicit val mat = ActorMaterializer()
  private implicit val ec = system.dispatcher


  "Loadbalancer" should "process all request with single endpoint" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)
    val latch = new TestLatch(3)

    val loadbalancer = Loadbalancer.singleRequests(endpointSource, LoadbalancerSettings.default)

    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }

    latch.await(5 seconds) shouldBe true

    mock.processed() shouldBe 3
    mock.unbind()
  }

}
