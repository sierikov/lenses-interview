package io.myawesome.fintech.forwarder.sink

import java.time.Instant

/** Abstract data sink
  * @tparam F
  *   Effect type
  * @tparam V
  *   Type of what
  */
trait Sink[F[_], V] {
  def sendBatch(events: List[(Instant, V)]): F[Unit]
}
