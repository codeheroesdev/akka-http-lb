package io.codeheroes.akka.http.lb

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

class LoadbalancerStageTest extends FlatSpec with Matchers {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  "LoadbalancerStage" should "adapt to endpoint failure" in {
    val latch = new TestLatch(10)
    val settings = LoadbalancerSettings(2, 10, createConnectionBuilder(5))

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
    val settings = LoadbalancerSettings(2, 10, createConnectionBuilder(10))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(responsesLatch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))

    responsesLatch.await(5 seconds) shouldBe true
    responsesLatch.reset(5)

    endpointsQueue.offer(EndpointDown(Endpoint("localhost", 9090)))
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

  /*Helper methods*/
  private def buildLoadbalancer(latch: TestLatch, settings: LoadbalancerSettings) = {
    val endpoints = Source.queue[EndpointEvent](1024, OverflowStrategy.backpressure)
    val requests = Source.queue[(HttpRequest, Int)](1024, OverflowStrategy.backpressure)
    val responses = Flow[(Try[HttpResponse], Int)].map(result => {
      latch.countDown()
      result
    }).toMat(Sink.seq)(Keep.right)

    RunnableGraph.fromGraph(GraphDSL.create(endpoints, requests, responses)((_, _, _)) { implicit builder =>
      (endpointsIn, requestsIn, responsesOut) =>
        import GraphDSL.Implicits._
        val lb = builder.add(new LoadbalancerStage[Int](settings))
        endpointsIn ~> lb.in0
        requestsIn ~> lb.in1
        lb.out ~> responsesOut.in
        ClosedShape
    }).run()
  }

  private def createConnectionBuilder(failAfter: Int): (Endpoint) => Flow[HttpRequest, HttpResponse, NotUsed] = {
    val processedRequest = new AtomicInteger(0)
    (endpoint: Endpoint) => Flow[HttpRequest].map { case _ =>
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