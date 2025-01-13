package io.myawesome.fintech.forwarder.exceptions

case class UpdateIndexException(index: String) extends IllegalStateException(
      s"Failed to update index $index",
    )
