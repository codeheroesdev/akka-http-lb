package io.codeheroes.akka.http.lb

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy, QueueOfferResult}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class Loadbalancer(endpointEventsSouce: Source[EndpointEvent, NotUsed], settings: LoadbalancerSettings)
                  (implicit system: ActorSystem, mat: ActorMaterializer) {

  private val inputSource = Source.queue[(HttpRequest, Promise[HttpResponse])](settings.requestsBufferSize, OverflowStrategy.dropNew)
  private val responsesSink = Sink.foreach[(Try[HttpResponse], Promise[HttpResponse])] {
    case (response, promise) => promise.completeWith(Future.fromTry(response))
  }

  //TODO: Handle stream error
  private val (_, inputQueue, stream) = RunnableGraph.fromGraph(GraphDSL.create(endpointEventsSouce, inputSource, responsesSink)((_, _, _)) { implicit builder =>
    (a, b, c) =>
      import GraphDSL.Implicits._
      val lb = builder.add(new LoadbalancerStage[Promise[HttpResponse]](settings))
      a ~> lb.in0
      b ~> lb.in1
      lb.out ~> c
      ClosedShape
  }).run()

  //TODO: Decide what exceptions should be thrown
  def singleRequest(request: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]()
    inputQueue.offer((request, promise)).flatMap {
      case QueueOfferResult.Dropped => Future.failed(BufferOverflowException)
      case QueueOfferResult.QueueClosed => Future.failed(BufferOverflowException)
      case QueueOfferResult.Enqueued => promise.future
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
    }
  }

}
