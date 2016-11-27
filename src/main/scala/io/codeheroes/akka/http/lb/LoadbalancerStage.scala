package io.codeheroes.akka.http.lb

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
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

  override def shape = new FanInShape2(endpointsIn, requestsIn, responsesOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val endpoints: mutable.Map[Endpoint, Int] = mutable.Map.empty[Endpoint, Int]
    private val slots: mutable.Queue[EndpointSlot] = mutable.Queue.empty[EndpointSlot]

    override def preStart(): Unit = {
      pull(endpointsIn)
      pull(requestsIn)
    }

    val endpointsHandler = new InHandler {
      override def onPush(): Unit = {
        grab(endpointsIn) match {
          case EndpointUp(endpoint) => handleNewEndpoint(endpoint)
          case EndpointDown(endpoint) => closeEndpointConnection(endpoint)
        }
        pull(endpointsIn)
      }

      override def onUpstreamFinish(): Unit = () //TODO: Handle endpoint upstream finish


      override def onUpstreamFailure(ex: Throwable): Unit = () //TODO: Handle endpoint upstream failure
    }


    def handleNewEndpoint(endpoint: Endpoint) = if (!endpoints.contains(endpoint)) {
      endpoints += (endpoint -> 0)
      ensureEndpointSlots(endpoint)
    }

    def ensureEndpointSlots(endpoint: Endpoint) =
      endpoints
        .get(endpoint)
        .map(settings.connectionsPerEndpoint - _)
        .map(lackedSlots => Vector.fill(lackedSlots)(new EndpointSlot(endpoint)))
        .foreach(newSlots => {
          newSlots.foreach(slots.enqueue(_))
          endpoints(endpoint) = endpoints(endpoint) + newSlots.size
        })

    def closeEndpointConnection(endpoint: Endpoint) = ()

    def tryHandleRequest() =
      if (endpoints.isEmpty && isAvailable(requestsIn) && isAvailable(responsesOut)) {
        val (_, result) = grab(requestsIn)
        push(responsesOut, (Failure(NoEndpointsAvailableException), result))
        pull(requestsIn)
      } else if (isAvailable(requestsIn)) {
        slots
          .dequeueFirst(_.isReadyToHandle)
          .foreach(slot => {
            slot.handleRequest(grab(requestsIn))
            slots.enqueue(slot)
            pull(requestsIn)
          })
      }

    def tryHandleResponse(slot: EndpointSlot) = {
      if (isAvailable(responsesOut)) {
        push(responsesOut, slot.getResponse)
      }
    }

    class EndpointSlot(endpoint: Endpoint) {
      slot =>
      private val slotInlet = new SubSinkInlet[HttpResponse](s"EndpointSlot.[$endpoint].in")
      private val slotOutlet = new SubSourceOutlet[HttpRequest](s"EndpointSlot.[$endpoint].out")
      private val connectionFlow = Http().outgoingConnection(endpoint.host, endpoint.port)

      private var inFlight = mutable.Queue.empty[T]

      slotInlet.setHandler(new InHandler {
        override def onPush(): Unit = tryHandleResponse(slot)

        override def onUpstreamFinish(): Unit = () //TODO: Handle connection flow in finish

        override def onUpstreamFailure(ex: Throwable): Unit = () //TODO: Handle connection flow in failure

      })

      slotOutlet.setHandler(new OutHandler {
        override def onPull(): Unit = tryHandleRequest()

        override def onDownstreamFinish(): Unit = () //TODO: Handle connection flow out finish
      })

      Source
        .fromGraph(slotOutlet.source)
        .via(connectionFlow)
        .runWith(Sink.fromGraph(slotInlet.sink))(subFusingMaterializer)

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
    }


    setHandler(endpointsIn, endpointsHandler)
    setHandler(requestsIn, new InHandler {
      override def onPush(): Unit = tryHandleRequest()

      override def onUpstreamFinish(): Unit = () //TODO: Handle requests in flow finish

      override def onUpstreamFailure(ex: Throwable): Unit = () //TODO: Handle requests in flow failure
    })

    setHandler(responsesOut, new OutHandler {
      override def onPull(): Unit =
        slots
          .dequeueFirst(_.isReadyToGet)
          .foreach(slot => {
            tryHandleResponse(slot)
            slots.enqueue(slot)
          })

      override def onDownstreamFinish(): Unit = () //TODO: Handle responses out flow finish
    })

  }


}
