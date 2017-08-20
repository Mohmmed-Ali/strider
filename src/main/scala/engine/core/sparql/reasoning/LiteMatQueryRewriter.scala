package engine.core.sparql.reasoning

import engine.core.label.SparkOpLabels
import engine.core.sparkop.compiler.SparkOpTransformer
import engine.core.sparkop.op.litematop.LiteMatBGP
import org.apache.jena.sparql.algebra.op.OpBGP

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 19/06/2017.
  */
class LiteMatQueryRewriter extends SparkOpTransformer {

  override def visit(opBGP: OpBGP): Unit = {
    opID += 1
    log.debug(s"opID: $opID, opBGP: $opBGP")

    val liteMatBGP = LiteMatBGP(opBGP, opBGP.getPattern.getList.toList)
    liteMatBGP.opName = SparkOpLabels.LITEMAT_BGP_NAME + opID
    stack.push(liteMatBGP)
  }
}

object LiteMatQueryRewriter {
  def apply: LiteMatQueryRewriter = new LiteMatQueryRewriter()
}