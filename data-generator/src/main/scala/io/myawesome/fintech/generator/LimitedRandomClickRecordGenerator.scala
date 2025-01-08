package io.myawesome.fintech.generator

import cats.*
import cats.effect.std.Random
import cats.syntax.all.*
import io.myawesome.fintech.common.ClickRecord

class LimitedRandomClickRecordGenerator[F[_] : Monad](using random: Random[F]) extends RandomClickRecordGenerator[F] {

  // can be passed / generated through config of generator. The same for other types
  private val uuids = List(
    "019445c6-3b09-77e7-8822-23477f09ddf7",
    "19445c6-6f45-7a2c-bde9-5ee049e16dbe",
    "019445c6-6f45-75a8-9caa-012b45f41f09",
    "019445c6-6f45-7616-8bfe-66b9b90f3bf4",
    "019445c6-6f45-7c44-b5db-a2e688cf13be"
  )

  private val browsers = List("Chrome", "Firefox", "Safari", "Edge", "Opera")
  private val campaign = List("Black Friday", "Christmas", "Unidays", "campaign A", "campaign B")
  private val channel = List("Email", "Social Media", "Search Engine", "Direct", "Affiliate")
  private val referrers = List("google.com", "facebook.com", "twitter.com", "linkedin.com", "none")
  
  private def randomOption[T](generator: F[T], chance: Double = 0.5): F[Option[T]] =
    random.nextDouble.map(_ < chance).ifM(generator.map(Some(_)), none[T].pure[F])
  
  private def randomIp(): F[String] = {
    for {
      a <- random.nextIntBounded(256)
      b <- random.nextIntBounded(256)
      c <- random.nextIntBounded(256)
      d <- random.nextIntBounded(256)
    } yield s"$a.$b.$c.$d"
  }

  override def generate: F[ClickRecord] = for {
    sessionId <- random.nextIntBounded(uuids.length).map(uuids)
    browser <- randomOption(random.nextIntBounded(browsers.length).map(browsers))
    campaign <- randomOption(random.nextIntBounded(campaign.length).map(campaign))
    channel <- random.nextIntBounded(channel.length).map(channel)
    referrer <- randomOption(random.nextIntBounded(referrers.length).map(referrers))
    ip <- randomOption(randomIp())
  } yield ClickRecord(session_id = sessionId, browser = browser, campaign = campaign, channel = channel, referrer = referrer, ip = ip)
}
