package io.codeheroes.akka.http.lb

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.codeheroes.akka.http.lb.core.{EndpointMock, TestLatch}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

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

    Thread.sleep(1000)

    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }

    latch.await(5 seconds) shouldBe true

    mock.processed() shouldBe 3
    mock.unbind()
  }

  it should "process single request with flow" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)

    val loadBalancerFlow = Loadbalancer.flow[Done](endpointSource, LoadbalancerSettings.default)

    val completed = Source.single((HttpRequest(), Done))
      .via(loadBalancerFlow)
      .runWith(Sink.seq)

    Await.result(completed, 3 seconds)

    mock.processed() shouldBe 1
    mock.unbind()
  }

  it should "process list of requests with flow" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)

    val loadBalancerFlow = Loadbalancer.flow[Done](endpointSource, LoadbalancerSettings.default)

    val completed = Source(List((HttpRequest(), Done), (HttpRequest(), Done), (HttpRequest(), Done)))
      .via(loadBalancerFlow)
      .runWith(Sink.seq)

    Await.result(completed, 3 seconds)
    mock.processed() shouldBe 3
    mock.unbind()
  }


}
