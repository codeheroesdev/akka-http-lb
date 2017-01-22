package io.codeheroes.akka.http.lb

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import io.codeheroes.akka.http.lb.core.TestLatch
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class LoadBalancerStageTest extends FlatSpec with Matchers {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  "LoadbalancerStage" should "adapt to endpoint failure" in {
    val latch = new TestLatch(10)
    val settings = LoadBalancerSettings(2, 10, 5 seconds, createConnectionBuilder(5))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))


    latch.await(5 seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe false
    result(10) shouldBe false

    result should have size 10
  }

  it should "adopt to endpoint down events" in {
    val responsesLatch = new TestLatch(5)
    val settings = LoadBalancerSettings(2, 10, 5 seconds, createConnectionBuilder(10))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(responsesLatch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))

    responsesLatch.await(5 seconds) shouldBe true
    responsesLatch.reset(5)

    endpointsQueue.offer(EndpointDown(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))

    responsesLatch.await(5 seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe false
    result(10) shouldBe false

    result should have size 10

  }

  it should "not drop endpoint after Timeout exception" in {
    val responsesLatch = new TestLatch(10)
    val connectionBuilder = (endpoint: Endpoint) => {
      val processedRequest = new AtomicInteger(0)
      Flow[HttpRequest].map { _ =>
        val processed = processedRequest.incrementAndGet()
        if (processed == 2 || processed == 3) throw new TimeoutException() else HttpResponse()
      }
    }

    val settings = LoadBalancerSettings(1, 0, 5 seconds, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(responsesLatch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))

    responsesLatch.await(5 seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe false
    result(3) shouldBe false
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe true
    result(7) shouldBe true
    result(8) shouldBe true
    result(9) shouldBe true
    result(10) shouldBe true

    result should have size 10

  }


  it should "not drop endpoint if not enough failures occurs within reset interval" in {
    val latch = new TestLatch(10)
    val processedRequest = new AtomicInteger(0)

    val connectionBuilder = (endpoint: Endpoint) => {
      Flow[HttpRequest].map { _ =>
        val processed = processedRequest.incrementAndGet()
        if (Set(6, 7, 8).contains(processed)) throw new IllegalArgumentException("Failed") else HttpResponse()
      }
    }
    val settings = LoadBalancerSettings(2, 3, 250 millis, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))

    Thread.sleep(500)

    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))


    latch.await(5 seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe true
    result(10) shouldBe true

    result should have size 10
  }

  it should "drop endpoint if too many failures occurs within reset interval" in {
    val latch = new TestLatch(10)
    val processedRequest = new AtomicInteger(0)

    val connectionBuilder = (endpoint: Endpoint) => {
      Flow[HttpRequest].map { _ =>
        val processed = processedRequest.incrementAndGet()
        if (Set(6, 7, 8).contains(processed)) throw new IllegalArgumentException("Failed") else HttpResponse()
      }
    }
    val settings = LoadBalancerSettings(2, 3, 5 seconds, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))


    latch.await(5 seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)


    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe false
    result(10) shouldBe false

    result should have size 10
  }

  /*Helper methods*/
  private def buildLoadbalancer(latch: TestLatch, settings: LoadBalancerSettings) = {
    val endpoints = Source.queue[EndpointEvent](1024, OverflowStrategy.backpressure)
    val requests = Source.queue[(HttpRequest, Int)](1024, OverflowStrategy.backpressure)
    val responses = Flow[(Try[HttpResponse], Int)].map(result => {
      latch.countDown()
      result
    }).toMat(Sink.seq)(Keep.right)

    RunnableGraph.fromGraph(GraphDSL.create(endpoints, requests, responses)((_, _, _)) { implicit builder =>
      (endpointsIn, requestsIn, responsesOut) =>
        import GraphDSL.Implicits._
        val lb = builder.add(new LoadBalancerStage[Int](settings))
        endpointsIn ~> lb.in0
        requestsIn ~> lb.in1
        lb.out ~> responsesOut.in
        ClosedShape
    }).run()
  }

  private def createConnectionBuilder(failAfter: Int): (Endpoint) => Flow[HttpRequest, HttpResponse, NotUsed] = {
    val processedRequest = new AtomicInteger(0)
    (endpoint: Endpoint) => Flow[HttpRequest].map { _ =>
      val processed = processedRequest.incrementAndGet()
      if (processed > failAfter) throw new IllegalStateException(s"Failed") else HttpResponse()
    }
  }

  private def convertIntoStatistics(responses: Future[Seq[(Try[HttpResponse], Int)]]) =
    Await.result(responses, 5 seconds).map {
      case (Success(_), i) => i -> true
      case (Failure(_), i) => i -> false
    }.toMap
}