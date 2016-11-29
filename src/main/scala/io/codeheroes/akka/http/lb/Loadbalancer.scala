package io.codeheroes.akka.http.lb

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._


object Loadbalancer {
  val DefaultSettings = LoadbalancerSettings(connectionsPerEndpoint = 32, maxEndpointFailures = 3)

  def flow[T](endpointEventsSource: Source[EndpointEvent, NotUsed], settings: LoadbalancerSettings = DefaultSettings)
             (implicit system: ActorSystem, mat: ActorMaterializer) =

    Flow.fromGraph(GraphDSL.create(endpointEventsSource) { implicit builder =>
      eventsInlet =>
        import GraphDSL.Implicits._
        val lb = builder.add(new LoadbalancerStage[T](settings))
        eventsInlet ~> lb.in0
        FlowShape(lb.in1, lb.out)
    })


  def singleRequests(endpointEventsSource: Source[EndpointEvent, NotUsed], settings: LoadbalancerSettings = DefaultSettings)
                    (implicit system: ActorSystem, mat: ActorMaterializer) =
    new SingleRequestLoadbalancer(endpointEventsSource, settings)
}