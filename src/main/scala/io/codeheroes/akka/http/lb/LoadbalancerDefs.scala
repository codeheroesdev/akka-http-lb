package io.codeheroes.akka.http.lb

final case class Endpoint(host: String, port: Int)

sealed trait EndpointEvent {
  def endpoint: Endpoint
}

final case class EndpointUp(endpoint: Endpoint) extends EndpointEvent
final case class EndpointDown(endpoint: Endpoint) extends EndpointEvent

case object NoEndpointsAvailableException extends Exception
case object BufferOverflowException extends Exception

case class LoadbalancerSettings(connectionsPerEndpoint: Int, requestsBufferSize: Int)