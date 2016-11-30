package io.codeheroes.akka.http.lb

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import org.scalatest.{FlatSpec, Matchers}
import scala.util.Try

class LoadbalancerStageTest extends FlatSpec with Matchers {

  "LoadbalancerStage" should "adapt to endpoint events" in {
    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    def connectionBuilder(endpoint: Endpoint) = Flow[HttpRequest].map(_ => HttpResponse())
    val endpoints = Source.queue[EndpointEvent](1024, OverflowStrategy.backpressure)
    val requests = Source.queue[(HttpRequest, Int)](1024, OverflowStrategy.backpressure)
    val responses = Sink.seq[(Try[HttpResponse], Int)]
    val settings = LoadbalancerSettings(2, 3, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = RunnableGraph.fromGraph(GraphDSL.create(endpoints, requests, responses)((_, _, _)) { implicit builder =>
      (endpointsIn, requestsIn, responsesOut) =>
        import GraphDSL.Implicits._
        val lb = builder.add(new LoadbalancerStage[Int](settings))
        endpointsIn ~> lb.in0
        requestsIn ~> lb.in1
        lb.out ~> responsesOut
        ClosedShape
    }).run()
  }

}