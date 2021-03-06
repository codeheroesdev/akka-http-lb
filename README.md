akka-http-lb
===

[![Build Status](https://travis-ci.org/codeheroesdev/akka-http-lb.svg?branch=master)](https://travis-ci.org/codeheroesdev/akka-http-lb)

Disclaimer
----------
This Akka HTTP loadbalancer is now under development and should be treated as alpha software.

Dependencies
------------
Add the following lines to your `build.sbt` file:

    resolvers += Resolver.bintrayRepo("codeheroes", "maven")

    libraryDependencies += "akka-http-lb" %% "akka-http-lb" % "0.5.0"

This version of `akka-http-lb` is not intend to be used on production.


Examples of LoadBalancer
------------------------
### flow
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

### single request API
```scala
  //Source of endpoint events
  val endpointEvents = Source(EndpointUp(Endpoint("hostA", 8080)) :: EndpointUp(Endpoint("hostB", 8080)) :: Nil)
  val settings = LoadBalancerSettings.default

  //Create the client which will distribute requests between hosts
  val loadBalancedClient = LoadBalancer.singleRequests(endpointEvents, settings)
  
  //Execute HttpRequest via client
  loadBalancedClient.request(HttpRequest(uri = "/")).onComplete{
    case Success(response) => //do something with response
    case Failure(ex) => //do something with failure
  }
```
