akka-http-lb
===

[![Build Status](https://travis-ci.org/codeheroesdev/akka-http-lb.svg?branch=master)](https://travis-ci.org/codeheroesdev/akka-http-lb)

## Disclaimer
This Akka HTTP loadbalancer is now under development and should be treated as alpha software.

##Example of LoadBalancer flow
```scala
  //Source of endpoint events
  val endpointEvents = Source(EndpointUp(Endpoint("hostA", 8080)) :: EndpointUp(Endpoint("hostB", 8080)) :: Nil)
  val settings = LoadBalancerSettings.default
  
  val loadBalancerFlow = LoadBalancer.flow[Int](endpointEvents, settings)
  
  //Source of HttpRequest and Int - Int value can be used to correlate requests with responses
  val requestsSource: Source[(HttpRequest, Int), NotUsed] = _

  //Sink of Try[HttpRequest] and Int - Int value can be used to correlate responses with requests
  val responsesSink: Sink[(Try[HttpResponse], Int), NotUsed] = _
  
  requestsSource.via(loadBalancerFlow).to(responsesSink).run()
```
