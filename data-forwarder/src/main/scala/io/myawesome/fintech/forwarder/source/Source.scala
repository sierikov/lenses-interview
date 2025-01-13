package io.myawesome.fintech.forwarder.source

import fs2.Stream

/** Abstract source of data events
  * @tparam F
  *   Effect
  * @tparam K
  *   Key type
  * @tparam V
  *   Value type (ClickRecord)
  */
trait Source[F[_], K, V] {

  /** Creates a stream of streams of event from id
    */
  def consume: Stream[F, Stream[F, DataRecord[F, K, V]]]
}
