package io.codeheroes.akka.http.lb

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class LoadbalancerStage[T](settings: LoadbalancerSettings)(implicit system: ActorSystem, mat: ActorMaterializer) extends GraphStage[FanInShape2[EndpointEvent, (HttpRequest, T), (Try[HttpResponse], T)]] {

  val endpointsIn = Inlet[EndpointEvent]("LoadbalancerStage.EndpointEvents.in")
  val requestsIn = Inlet[(HttpRequest, T)]("LoadbalancerStage.Requests.in")
  val responsesOut = Outlet[(Try[HttpResponse], T)]("LoadbalancerStage.Responses.out")
  val log = system.log

  override def shape = new FanInShape2(endpointsIn, requestsIn, responsesOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val endpoints: mutable.Map[Endpoint, Int] = mutable.Map.empty
    private val endpointsFailures: mutable.Map[Endpoint, Int] = mutable.Map.empty
    private val slots: mutable.Queue[EndpointSlot] = mutable.Queue.empty
    private val failedResponses: mutable.Queue[(Try[HttpResponse], T)] = mutable.Queue.empty
    private var connectionSlotId = 0

    override def preStart(): Unit = {
      pull(endpointsIn)
      pull(requestsIn)
    }

    val endpointsHandler = new InHandler {
      override def onPush(): Unit = {
        grab(endpointsIn) match {
          case EndpointUp(endpoint) => handleNewEndpoint(endpoint)
          case EndpointDown(endpoint) => dropEndpoint(endpoint)
        }
        pull(endpointsIn)
      }

      override def onUpstreamFinish(): Unit = ()

      override def onUpstreamFailure(ex: Throwable): Unit =
        failStage(throw new IllegalStateException(s"EndpointEvents stream failed", ex))
    }

    def nextConnectionSlotId() = {
      connectionSlotId += 1
      connectionSlotId
    }

    def handleNewEndpoint(endpoint: Endpoint) =
      if (!endpoints.contains(endpoint)) {
        endpoints += (endpoint -> 0)
        tryHandleRequest(tryEnsureSlots = true)
      }

    def ensureSlots(): Unit = {
      endpoints
        .find { case (_, count) => count < settings.connectionsPerEndpoint }
        .foreach { case (endpoint, currentCount) =>
          slots.enqueue(new EndpointSlot(endpoint, nextConnectionSlotId()))
          endpoints(endpoint) = currentCount + 1
        }

      tryHandleRequest(tryEnsureSlots = false)
    }

    def tryHandleRequest(tryEnsureSlots: Boolean) =
      if (endpoints.isEmpty && isAvailable(requestsIn)) {
        val (_, result) = grab(requestsIn)
        failedResponses.enqueue((Failure(NoEndpointsAvailableException), result))
        tryHandleResponseFromFailed()
        pull(requestsIn)
      } else if (isAvailable(requestsIn)) {
        slots.dequeueFirst(_.isReadyToHandle) match {
          case Some(slot) =>
            slot.handleRequest(grab(requestsIn))
            slots.enqueue(slot)
            pull(requestsIn)

          case None => if (tryEnsureSlots) ensureSlots()
        }
      }

    def tryHandleResponseFromSlot(slot: EndpointSlot) =
      if (isAvailable(responsesOut)) {
        push(responsesOut, slot.getResponse)
      }

    def tryHandleResponseFromFailed() =
      if (isAvailable(responsesOut) && failedResponses.nonEmpty) {
        push(responsesOut, failedResponses.dequeue())
      }


    def slotFailed(slot: EndpointSlot, cause: Throwable) = {
      val slotEndpoint = slot.endpoint
      val totalFailure = endpointsFailures.getOrElse(slotEndpoint, 0) + 1
      endpointsFailures(slotEndpoint) = totalFailure

      log.error(cause, s"Slot ${slot.id} to $slotEndpoint failed")

      if (endpointsFailures(slotEndpoint) >= settings.maxEndpointFailures) {
        log.error(cause, s"Dropping $slotEndpoint, failed $totalFailure times")
        dropEndpoint(slotEndpoint, Some(cause))
      } else {
        removeSlot(slot, Some(cause))
      }
    }

    def dropEndpoint(endpoint: Endpoint, cause: Option[Throwable] = None) = {
      endpoints.remove(endpoint)
      endpointsFailures.remove(endpoint)
      slots.dequeueAll(_.endpoint == endpoint).foreach(_.completeSlot(cause))
      tryHandleResponseFromFailed()
      tryHandleRequest(tryEnsureSlots = true)
    }

    def removeSlot(slot: EndpointSlot, cause: Option[Throwable]) = {
      endpoints(slot.endpoint) = endpoints(slot.endpoint) - 1
      slots.dequeueAll(_.id == slot.id).foreach(_.completeSlot(cause))
      tryHandleResponseFromFailed()
      tryHandleRequest(tryEnsureSlots = true)
    }

    class EndpointSlot(val endpoint: Endpoint, val id: Int) {
      slot =>
      private val slotInlet = new SubSinkInlet[HttpResponse](s"EndpointSlot.[$endpoint].[$id].in")
      private val slotOutlet = new SubSourceOutlet[HttpRequest](s"EndpointSlot.[$endpoint].[$id].out")

      private val inFlight = mutable.Queue.empty[T]

      slotInlet.setHandler(new InHandler {
        override def onPush(): Unit = tryHandleResponseFromSlot(slot)

        override def onUpstreamFinish(): Unit = removeSlot(slot, None)

        override def onUpstreamFailure(ex: Throwable): Unit = slotFailed(slot, ex)
      })

      slotOutlet.setHandler(new OutHandler {
        override def onPull(): Unit = tryHandleRequest(tryEnsureSlots = true)

        override def onDownstreamFinish(): Unit = () //Ignored in order to handled upstream failure
      })

      Source
        .fromGraph(slotOutlet.source)
        .via(settings.connectionBuilder(endpoint))
        .runWith(Sink.fromGraph(slotInlet.sink))(mat)

      slotInlet.pull()

      def handleRequest(request: (HttpRequest, T)) = {
        inFlight.enqueue(request._2)
        slotOutlet.push(request._1)
      }

      def getResponse = {
        val response = slotInlet.grab()
        val element = inFlight.dequeue()
        slotInlet.pull()
        (Success(response), element)
      }

      def isReadyToHandle = slotOutlet.isAvailable

      def isReadyToGet = slotInlet.isAvailable

      def completeSlot(causeOpt: Option[Throwable] = None) = {
        val ex = causeOpt match {
          case Some(cause) => new IllegalStateException(s"Failure to process request to $endpoint at slot $id", cause)
          case None => new IllegalStateException(s"Failure to process request to $endpoint at slot $id")
        }
        slotInlet.cancel()
        slotOutlet.complete()

        inFlight.dequeueAll(_ => true).foreach(t => {
          failedResponses.enqueue((Failure(ex), t))
        })
      }
    }

    setHandler(endpointsIn, endpointsHandler)
    setHandler(requestsIn, new InHandler {
      override def onPush(): Unit = tryHandleRequest(tryEnsureSlots = true)

      override def onUpstreamFinish(): Unit = completeStage()

      override def onUpstreamFailure(ex: Throwable): Unit =
        failStage(throw new IllegalStateException(s"Requests stream failed", ex))
    })

    setHandler(responsesOut, new OutHandler {
      override def onPull(): Unit =
        if (failedResponses.nonEmpty) {
          push(responsesOut, failedResponses.dequeue())
        } else {

          slots
            .dequeueFirst(_.isReadyToGet)
            .foreach(slot => {
              tryHandleResponseFromSlot(slot)
              slots.enqueue(slot)
            })
        }


      override def onDownstreamFinish(): Unit = completeStage()
    })

  }
}
