package engine.core.sparql.reasoning

/**
  * Created by xiangnanren on 04/07/2017.
  */
trait LiteMatCtxBuilder {

  private val opSettingsKey = Seq(
    "litemat.dct.concepts",
    "litemat.dct.properties",
    "litemat.dct.individuals")

  /**
    * LiteMat context allows to redefine the path of the dictionaries of
    * concepts, properties and individuals. To do this, invoke:
    *
    *   LiteMatCtx.
    *     set("litemat.dct.concepts", CPT_DCT_PATH).
    *     set("litemat.dct.properties", PROP_DCT_PATH).
    *     set("litemat.dct.individuals", IND_DCT_PATH)
    *
    * Note that the dictionary of individuals is not always required,
    * it is used for the reasoning of sameAs.
    *
    */
  protected def set(key: String, value: String): LiteMatCtx.type = {
    if (key == null) {
      throw new NullPointerException("Null key for settings")
    }
    if (!opSettingsKey.contains(key)) {
      throw new NullPointerException("Invalid key for settings")
    }
    if (value == null) {
      throw new NullPointerException("Null value for settings")
    }
    LiteMatCtx.settings.put(key, value)
    LiteMatCtx
  }

}
