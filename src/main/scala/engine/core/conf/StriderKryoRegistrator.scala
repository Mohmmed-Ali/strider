package engine.core.conf

import com.esotericsoftware.kryo.Kryo
import engine.core.sparkexpr.expr.SparkExpr
import engine.core.sparql.reasoning.{Decoder, Encoder}
import engine.core.sparkop.executor.{ConstructExecutor, LiteMatConstructExecutor}
import engine.stream.{RDFGraph, RDFTriple}
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
    kryo.register(classOf[Array[Object]])
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"))
    kryo.register(Class.forName("[[B"))
    kryo.register(classOf[Encoder])
    kryo.register(classOf[Decoder])
    kryo.register(classOf[ConstructExecutor])
    kryo.register(classOf[LiteMatConstructExecutor])
    kryo.register(classOf[SparkExpr])
    kryo.register(classOf[RDFTriple])
    kryo.register(classOf[Array[RDFTriple]])
    kryo.register(classOf[RDFGraph])
    kryo.register(classOf[Model])
    kryo.register(classOf[org.apache.jena.graph.Triple])
    kryo.register(classOf[org.apache.jena.query.Query])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.expressions.UnsafeRow]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])
    kryo.register(classOf[Array[org.apache.spark.sql.Row]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"))
    kryo.register(Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))

  }
}
