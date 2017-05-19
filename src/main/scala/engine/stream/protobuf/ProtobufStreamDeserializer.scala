package engine.stream.protobuf

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/**
  * Created by xiangnanren on 18/05/2017.
  */
class ProtobufStreamDeserializer[T](props: VerifiableProperties = null)
  extends Decoder[T]{
  override def fromBytes(bytes: Array[Byte]): T = ???
}
