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

class LoadBalancerTests extends FlatSpec with Matchers {
  private implicit val system = ActorSystem()
  private implicit val mat = ActorMaterializer()
  private implicit val ec = system.dispatcher


  "LoadBalancer" should "process all request with single endpoint" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)
    val latch = new TestLatch(3)

    val loadbalancer = LoadBalancer.singleRequests(endpointSource, LoadBalancerSettings.default)

    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }

    latch.await(5 seconds) shouldBe true

    mock.processed() shouldBe 3
    mock.unbind()
  }

  it should "process many single requests" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)
    val latch = new TestLatch(30)

    val loadbalancer = LoadBalancer.singleRequests(endpointSource, LoadBalancerSettings.default.copy(connectionsPerEndpoint = 8))

    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }

    Thread.sleep(250)

    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }

    Thread.sleep(250)

    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }
    loadbalancer.request(HttpRequest()).onSuccess { case _ => latch.countDown() }

    latch.await(10 seconds) shouldBe true

    mock.processed() shouldBe 30
    mock.unbind()
  }

  it should "process single request with flow" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)

    val loadBalancerFlow = LoadBalancer.flow[Done](endpointSource, LoadBalancerSettings.default)

    val completed = Source.single((HttpRequest(), Done))
      .via(loadBalancerFlow)
      .runWith(Sink.seq)

    Await.result(completed, 3 seconds) should have size 1

    mock.processed() shouldBe 1
    mock.unbind()
  }

  it should "process list of requests with flow" in {
    val endpoint = Endpoint("localhost", 31000)
    val endpointSource = Source(EndpointUp(endpoint) :: Nil)
    val mock = new EndpointMock(endpoint)

    val loadBalancerFlow = LoadBalancer.flow[Done](endpointSource, LoadBalancerSettings.default)

    val completed = Source(List((HttpRequest(), Done), (HttpRequest(), Done), (HttpRequest(), Done)))
      .via(loadBalancerFlow)
      .runWith(Sink.seq)

    Await.result(completed, 3 seconds) should have size 3
    mock.processed() shouldBe 3
    mock.unbind()
  }

  it should "distribute requests between endpoints" in {
    val endpoint1 = Endpoint("localhost", 31001)
    val endpoint2 = Endpoint("localhost", 31002)
    val endpoint3 = Endpoint("localhost", 31003)

    val mock1 = new EndpointMock(endpoint1)
    val mock2 = new EndpointMock(endpoint2)
    val mock3 = new EndpointMock(endpoint3)

    val endpointSource = Source(EndpointUp(endpoint1):: EndpointUp(endpoint2):: EndpointUp(endpoint3) :: Nil)
    val loadBalancerFlow = LoadBalancer.flow[Int](endpointSource, LoadBalancerSettings.default.copy(connectionsPerEndpoint = 4))
    val requests = (1 to 30).map(i => (HttpRequest(), i))

    val completed = Source(requests).via(loadBalancerFlow).runWith(Sink.seq)

    Await.result(completed, 5 seconds).size should be >= 28
    mock1.processed() should be >= 9
    mock2.processed() should be >= 9
    mock3.processed() should be >= 9
  }


}
