package io.codeheroes.akka.http.lb

import akka.Done
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
  * @author mciolek
  */

object EndpointStage {
  def flow[T](endpoint: Endpoint, stopSwitch: Future[Unit], settings: LoadBalancerSettings)(implicit ec: ExecutionContext) =
    Flow.fromGraph(new EndpointStage[T](endpoint, stopSwitch, settings))
}

class EndpointStage[T](endpoint: Endpoint, stopSwitch: Future[Unit], settings: LoadBalancerSettings)(implicit ec: ExecutionContext)
  extends GraphStage[FlowShape[(HttpRequest, T), (Try[HttpResponse], T)]] {

  private val in = Inlet[(HttpRequest, T)](s"EndpointStage.$endpoint.in")
  private val out = Outlet[(Try[HttpResponse], T)](s"EndpointStage.$endpoint.out")

  override def shape: FlowShape[(HttpRequest, T), (Try[HttpResponse], T)] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with InHandler with OutHandler {
    val connectionFlowSource = new SubSourceOutlet[(HttpRequest, T)]("EndpointStage.subSource")
    val connectionFlowSink = new SubSinkInlet[(Try[HttpResponse], T)]("EndpointStage.subSink")
    var errorCount = 0
    var inFlightRequest = 0
    var stopped = false
    val slotStopSwitch = Promise[Unit]()

    override def preStart(): Unit = {

      val flow = Flow.fromGraph(GraphDSL.create[FlowShape[(HttpRequest, T), (Try[HttpResponse], T)]]() { implicit b ⇒
        import GraphDSL.Implicits._

        val slots = Vector.tabulate(settings.connectionsPerEndpoint)(id => new LoadBalancerSlot[T](endpoint, id, handleError, slotStopSwitch.future, settings.connectionBuilder(endpoint)))
        val responseMerge = b.add(Merge[(Try[HttpResponse], T)](settings.connectionsPerEndpoint))
        val requestBalance = b.add(Balance[(HttpRequest, T)](settings.connectionsPerEndpoint))
        slots.zipWithIndex.foreach { case (slot, id) => requestBalance.out(id) ~> slot ~> responseMerge.in(id) }

        FlowShape(requestBalance.in, responseMerge.out)
      })


      val stopCallback = getAsyncCallback[Unit]((_) => {
        stopped = true
        slotStopSwitch.success(())
        tryComplete()
      })

      Source.fromGraph(connectionFlowSource.source).via(flow).runWith(Sink.fromGraph(connectionFlowSink.sink))(subFusingMaterializer)

      connectionFlowSink.pull()
      stopSwitch.foreach(stopCallback.invoke)
      schedulePeriodically(Done, settings.endpointFailuresResetInterval)
    }


    connectionFlowSource.setHandler(new OutHandler {
      override def onPull(): Unit =
        if (isAvailable(in) && !stopped) {
          connectionFlowSource.push(grab(in))
          inFlightRequest += 1
          pull(in)
        } else if (!hasBeenPulled(in)) {
          pull(in)
        }
    })

    connectionFlowSink.setHandler(new InHandler {
      override def onPush(): Unit = if (isAvailable(out)) {
        push(out, connectionFlowSink.grab())
        inFlightRequest -= 1
        tryComplete(() => connectionFlowSink.pull())
      }
    })

    override def onPull(): Unit =
      if (connectionFlowSink.isAvailable) {
        push(out, connectionFlowSink.grab())
        inFlightRequest -= 1
        tryComplete(() => connectionFlowSink.pull())
      } else if (!connectionFlowSink.hasBeenPulled) {
        connectionFlowSink.pull()
      }


    override def onPush(): Unit =
      if (connectionFlowSource.isAvailable && !stopped) {
        connectionFlowSource.push(grab(in))
        inFlightRequest += 1
        pull(in)
      }


    //TODO: Remove it and replace with event driven approach
    def handleError(id: Int, ex: Throwable): Unit = {
      errorCount += 1
      if (errorCount >= settings.maxEndpointFailures) {
        stopped = true
        slotStopSwitch.failure(new IllegalStateException(s"Too many failures for endpoint $endpoint"))
      }
    }

    def tryComplete(or: () => Unit = () => ()) = if (stopped && inFlightRequest <= 0) completeStage() else or()

    override def onTimer(timerKey: Any): Unit = errorCount = 0

    setHandler(in, this)
    setHandler(out, this)

  }


}
