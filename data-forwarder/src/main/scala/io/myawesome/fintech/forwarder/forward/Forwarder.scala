package io.myawesome.fintech.forwarder.forward

import fs2.Stream

trait Forwarder[F[_], K, V] {
  def forward(topic: String): Stream[F, Unit]
}
