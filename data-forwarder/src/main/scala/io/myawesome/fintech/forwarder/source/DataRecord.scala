package io.myawesome.fintech.forwarder.source

import fs2.kafka.CommittableOffset

import java.time.Instant

final case class DataRecord[F[_], K, V](
  key:       K,
  value:     V,
  partition: Int,
  timestamp: Instant,
  offset:    CommittableOffset[F],
)
