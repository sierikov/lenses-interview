package io.myawesome.fintech.forwarder.source

import fs2.Stream
import fs2.kafka.CommittableConsumerRecord

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
    * @param id
    *   id of partition of origin of data (for example topic name)
    */
  def consume(id: String): Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]]
}
