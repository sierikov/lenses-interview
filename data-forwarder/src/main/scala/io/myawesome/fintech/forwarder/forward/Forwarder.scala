package io.myawesome.fintech.forwarder.forward

import fs2.Stream
import io.myawesome.fintech.forwarder.sink.Sink
import io.myawesome.fintech.forwarder.source.Source

trait Forwarder[F[_], K, V] {
  def forward(source: Source[F, K, V], sink: Sink[F, V]): F[Unit]
}
