package engine.stream

import com.esotericsoftware.kryo.io.Input
import engine.core.conf.StriderDeserializerConf
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/**
  * Created by xiangnanren on 16/11/2016.
  */

class StreamDeserializer[T](props: VerifiableProperties = null) extends Decoder[T] {

  override def fromBytes(messageBytes: Array[Byte]): T = {

    val input = new Input()
    input.setBuffer(messageBytes)

    val message = StriderDeserializerConf.kryos.get().readClassAndObject(input)
    val messageObject = message.asInstanceOf[T]

    messageObject
  }
}

