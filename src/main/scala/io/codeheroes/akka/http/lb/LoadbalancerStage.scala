package io.codeheroes.akka.http.lb

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage._

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.{Failure, Try}

class LoadBalancerStage[T](settings: LoadBalancerSettings)(implicit system: ActorSystem, mat: ActorMaterializer)
  extends GraphStage[FanInShape2[EndpointEvent, (HttpRequest, T), (Try[HttpResponse], T)]] {
  val endpointsIn = Inlet[EndpointEvent]("LoadBalancerStage.EndpointEvents.in")
  val requestsIn = Inlet[(HttpRequest, T)]("LoadBalancerStage.Requests.in")
  val responsesOut = Outlet[(Try[HttpResponse], T)]("LoadBalancerStage.Responses.out")
  var firstRequest: (HttpRequest, T) = null
  var finished = false

  override def shape = new FanInShape2(endpointsIn, requestsIn, responsesOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    private val endpoints: mutable.Queue[EndpointWrapper] = mutable.Queue.empty
    private var inFlight = 0

    override def preStart(): Unit = {
      pull(endpointsIn)
      pull(requestsIn)
    }

    val endpointsInHandler = new InHandler {
      override def onPush(): Unit = tryHandleEndpoint()
      override def onUpstreamFinish(): Unit = ()
      override def onUpstreamFailure(ex: Throwable): Unit =
        failStage(throw new IllegalStateException(s"EndpointEvents stream failed", ex))
    }

    override def onPush(): Unit = tryHandleRequest()
    override def onPull(): Unit = tryHandleResponse()

    private def tryHandleEndpoint() = {
      grab(endpointsIn) match {
        case EndpointUp(endpoint) =>
          if (!endpoints.exists(_.endpoint == endpoint)) {
            endpoints.enqueue(new EndpointWrapper(endpoint))
          }
        case EndpointDown(endpoint) => endpoints.dequeueAll(_.endpoint == endpoint).foreach(_.disconnect())
      }
      pull(endpointsIn)
    }

    private def tryHandleRequest(): Unit =
      if (endpoints.isEmpty && isAvailable(responsesOut) && !isClosed(requestsIn) && !isClosed(responsesOut)) {
        if (firstRequest != null) {
          push(responsesOut, (Failure(NoEndpointsAvailableException), firstRequest._2))
          firstRequest = null
          inFlight -= 1
        } else if (isAvailable(requestsIn)) {
          val (_, result) = grab(requestsIn)
          push(responsesOut, (Failure(NoEndpointsAvailableException), result))
          pull(requestsIn)
        }
      } else if (firstRequest != null) {
        endpoints.find(_.isInAvailable).foreach(endpoint => {
          endpoint.push(firstRequest)
          firstRequest = null
        })
      } else if (isAvailable(requestsIn) && !isClosed(requestsIn)) {
        endpoints.find(_.isInAvailable) match {
          case Some(endpoint) =>
            endpoint.push(grab(requestsIn))
            pull(requestsIn)
            inFlight += 1
          case None =>
            firstRequest = grab(requestsIn)
            pull(requestsIn)
            inFlight += 1
        }
      }

    private def tryHandleResponse(): Unit = {
      if (endpoints.nonEmpty && isAvailable(responsesOut) && !isClosed(responsesOut)) {
        endpoints.find(_.isOutAvailable).foreach(endpoint => {
          push(responsesOut, endpoint.grabAndPull())
          inFlight -= 1
        })
      } else {
        tryHandleRequest()
      }
      tryFinish()
    }

    private def tryFinish() =
      if (finished && inFlight <= 0) {
        completeStage()
      }

    private def removeEndpoint(endpoint: Endpoint) = {
      endpoints.dequeueAll(_.endpoint == endpoint)
      tryHandleRequest()
    }

    class EndpointWrapper(val endpoint: Endpoint) {
      private val endpointSource = new SubSourceOutlet[(HttpRequest, T)](s"LoadBalancerStage.$endpoint.Source")
      private val endpointSink = new SubSinkInlet[(Try[HttpResponse], T)](s"LoadBalancerStage.$endpoint.Sink")
      private val stopSwitch = Promise[Unit]()
      private val stage = EndpointStage.flow[T](endpoint, stopSwitch.future, stop, settings)(mat)
      private var stopped = false

      private val inHandler = new InHandler {
        override def onPush(): Unit = tryHandleResponse()

        override def onUpstreamFinish(): Unit = removeEndpoint(endpoint)
      }

      private val outHandler = new OutHandler {
        override def onPull(): Unit = tryHandleRequest()

        override def onDownstreamFinish(): Unit = ()
      }

      private def stop(ex: Throwable): Unit = if (!stopped) {
        stopped = true
        stopSwitch.failure(ex)
      }

      endpointSource.setHandler(outHandler)
      endpointSink.setHandler(inHandler)
      Source.fromGraph(endpointSource.source).via(stage).runWith(Sink.fromGraph(endpointSink.sink))(subFusingMaterializer)
      endpointSink.pull()


      def push(element: (HttpRequest, T)) = {
        endpointSource.push(element)
      }

      def grabAndPull() = {
        val element = endpointSink.grab()
        endpointSink.pull()
        element
      }

      def isInAvailable = !stopped && endpointSource.isAvailable

      def isOutAvailable = endpointSink.isAvailable

      def disconnect() = stop(new IllegalStateException(s"Endpoint $endpoint down"))

    }

    setHandler(endpointsIn, endpointsInHandler)
    setHandler(requestsIn, this)
    setHandler(responsesOut, this)

    override def onUpstreamFinish(): Unit = {
      finished = true
      tryFinish()
    }

    override def onDownstreamFinish(): Unit = completeStage()
  }


}
