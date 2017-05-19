package engine.stream.protobuf

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties

/**
  * Created by xiangnanren on 18/05/2017.
  */
class ProtobufStreamSerializer[T](props: VerifiableProperties = null)
  extends Encoder[T] {
  override def toBytes(t: T): Array[Byte] = ???
}
