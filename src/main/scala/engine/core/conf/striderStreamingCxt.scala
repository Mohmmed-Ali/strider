package engine.core.conf

import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}

class StriderStreamingCxt(val batch: Duration,
                          val slide: Duration = Milliseconds(0L),
                          val window: Duration = Milliseconds(0L)) extends StriderCxtResolver {
  val updateFrequency =
    if (slide.isZero) batch.milliseconds
    else slide.milliseconds

  def getStreamingCtx(striderConf: StriderConfBase): StreamingContext = {
    new StreamingContext(
      new StriderConfBase().conf, batch)
  }
}

