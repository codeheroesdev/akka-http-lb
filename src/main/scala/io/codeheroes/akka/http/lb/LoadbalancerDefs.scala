package io.codeheroes.akka.http.lb

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Flow

final case class Endpoint(host: String, port: Int)

sealed trait EndpointEvent {
  def endpoint: Endpoint
}

final case class EndpointUp(endpoint: Endpoint) extends EndpointEvent
final case class EndpointDown(endpoint: Endpoint) extends EndpointEvent

case object NoEndpointsAvailableException extends Exception
case object BufferOverflowException extends Exception
case object RequestsQueueClosed extends Exception

case class LoadbalancerSettings(
                                 connectionsPerEndpoint: Int,
                                 maxEndpointFailures: Int,
                                 connectionBuilder: (Endpoint) => Flow[HttpRequest, HttpResponse, NotUsed]
                               )

case object LoadbalancerSettings {
  def default(implicit system: ActorSystem) =
    LoadbalancerSettings(
      connectionsPerEndpoint = 32,
      maxEndpointFailures = 8,
      (endpoint: Endpoint) => Http().outgoingConnection(endpoint.host, endpoint.port).mapMaterializedValue(_ => NotUsed)
    )
}
