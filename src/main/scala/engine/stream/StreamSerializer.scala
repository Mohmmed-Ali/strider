package engine.stream

import java.io.ByteArrayOutputStream

import engine.core.conf.StriderSerializerConf
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties

/**
  * Created by xiangnanren on 16/11/2016.
  */
class StreamSerializer[T](props: VerifiableProperties = null) extends Encoder[T] {

  override def toBytes(t: T): Array[Byte] = {
    val byteArrayOutput = new ByteArrayOutputStream()

    val output = StriderSerializerConf.kryoSerializer.newKryoOutput()
    output.setOutputStream(byteArrayOutput)

    StriderSerializerConf.kryos.get().writeClassAndObject(output, t)

    output.close()
    byteArrayOutput.toByteArray
  }
}
