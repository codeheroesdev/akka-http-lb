package io.codeheroes.akka.http.lb

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class SingleRequestLoadbalancer(endpointEventsSouce: Source[EndpointEvent, NotUsed], settings: LoadbalancerSettings)
                               (implicit system: ActorSystem, mat: ActorMaterializer) {

  private val inputSource = Source.queue[(HttpRequest, Promise[HttpResponse])](1024, OverflowStrategy.dropNew)
  private val responsesSink = Sink.foreach[(Try[HttpResponse], Promise[HttpResponse])] {
    case (response, promise) => promise.completeWith(Future.fromTry(response))
  }

  private val inputQueue = inputSource
    .via(Loadbalancer.flow(endpointEventsSouce, settings))
    .toMat(responsesSink)(Keep.left)
    .run()

  def request(request: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]()
    inputQueue.offer((request, promise)).flatMap {
      case QueueOfferResult.Dropped => Future.failed(RequestsQueueClosed)
      case QueueOfferResult.QueueClosed => Future.failed(RequestsQueueClosed)
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.Enqueued => promise.future
    }
  }

}