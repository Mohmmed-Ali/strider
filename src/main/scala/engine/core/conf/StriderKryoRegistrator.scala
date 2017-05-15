package engine.core.conf

import com.esotericsoftware.kryo.Kryo
import engine.stream.{RDFGraph, RDFTriple, WavesEvent}
import org.apache.jena.rdf.model.Model
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by xiangnanren on 16/11/2016.
  */

/**
  * Register the classes for kryo serializer
  * All the listed classes must be registered.
  */
class StriderKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[Array[String]])
    kryo.register(Class.forName("[[B"))
    kryo.register(classOf[RDFTriple])
    kryo.register(classOf[RDFGraph])
    kryo.register(classOf[WavesEvent])
    kryo.register(classOf[Model])
    kryo.register(classOf[org.apache.jena.graph.Triple])
    kryo.register(classOf[org.apache.jena.query.Query])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.expressions.UnsafeRow]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[Array[org.apache.spark.sql.Row]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"))
    kryo.register(Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))
  }
}
