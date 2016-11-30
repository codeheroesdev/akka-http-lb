package io.codeheroes.akka.http.lb.core

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration.Duration


class TestLatch(expectedCount: Int) {
  private var _latch = new CountDownLatch(expectedCount)

  def await(duration: Duration) = _latch.await(duration.toMillis, TimeUnit.MILLISECONDS)
  def countDown() = _latch.countDown()
  def reset(value: Int) = _latch = new CountDownLatch(value)
}