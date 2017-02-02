package io.codeheroes.akka.http.lb

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.codeheroes.akka.http.lb.core.EndpointMock
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class LoadBalancerLoadTests extends FlatSpec with Matchers {
  private implicit val system = ActorSystem()
  private implicit val mat = ActorMaterializer()
  private implicit val ec = system.dispatcher


  "LoadBalancer" should "loadbalance requests" in {
    val endpoint1 = Endpoint("localhost", 31001)
    val endpoint2 = Endpoint("localhost", 31002)
    val endpoint3 = Endpoint("localhost", 31003)
    val endpoint4 = Endpoint("localhost", 31004)


    val mock1 = new EndpointMock(endpoint1)
    val mock2 = new EndpointMock(endpoint2)
    val mock3 = new EndpointMock(endpoint3)
    val mock4 = new EndpointMock(endpoint4)


    val endpointSource = Source(EndpointUp(endpoint1) :: EndpointUp(endpoint2) :: EndpointUp(endpoint3) :: EndpointUp(endpoint4) :: Nil)
    val loadBalancerFlow = LoadBalancer.flow[Done](endpointSource, LoadBalancerSettings.default.copy(connectionsPerEndpoint = 256))


    val requests = Source.repeat((HttpRequest(), Done))

    val count = new AtomicInteger()

    val start = System.currentTimeMillis()

    val completed = requests.takeWithin(120 seconds).via(loadBalancerFlow).runWith(Sink.foreachParallel(2048)(e => count.incrementAndGet()))

    Await.result(completed, 300 seconds)

    val stop = System.currentTimeMillis()

    val total = mock1.processed() + mock2.processed() + mock3.processed() + mock4.processed()

    println(mock1.processed())
    println(mock2.processed())
    println(mock3.processed())
    println(mock4.processed())

    println(total / ((stop - start) / 1000))
    println(count.get() / ((stop - start) / 1000))

    println(total)
    println(count.get())

  }


}
