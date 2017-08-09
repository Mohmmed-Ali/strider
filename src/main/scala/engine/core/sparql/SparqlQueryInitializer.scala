package engine.core.sparql

import org.apache.spark.sql._

import scala.io.Source

/**
  * Created by xiangnanren on 07/07/16.
  */
trait QueryInitializer {
  def initializeQueryStr(queryId: String): String
}

class QueryReader extends QueryInitializer{

  override def initializeQueryStr(path: String): String = {
    val in = classOf[QueryInitializer].getResourceAsStream(path)
    val queryStr = Source.fromInputStream(in, "utf-8").getLines().mkString
    queryStr
  }
}


object NonLiteMatQueryProcessor {
  def process(queryId: String, rawDataFrame: DataFrame): DataFrame = {
    queryId match {
      case "nonLiteMat_q_1" =>
        val res1 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>")
        val res2 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>").
          withColumnRenamed("oDefault","professor").join(res1, Seq("sDefault"))
        val res3 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>").
          withColumnRenamed("oDefault","professor").join(res1, Seq("sDefault"))
        val res4 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>").
          withColumnRenamed("oDefault","professor").join(res1, Seq("sDefault"))
        res2.union(res3).union(res4)

      case "nonLiteMat_q_2" =>
        val advs = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>")
        val nmst = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>").withColumnRenamed("oDefault","ns")
        val nmpr = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>").withColumnRenamed("oDefault","nx")
        val assiP = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>").withColumnRenamed("oDefault","professor")
        val assoP = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>").withColumnRenamed("oDefault","professor")
        val fullP = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>").withColumnRenamed("oDefault","professor")
        val undeS = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent>")
        val gradS = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")

        val res1 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).select("ns","nx")
        val res2 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).select("ns","nx")
        val res3 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).select("ns","nx")
        val res4 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).select("ns","nx")
        val res5 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).select("ns","nx")
        val res6 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).select("ns","nx")
        res1.union(res2).union(res3).union(res4).union(res5).union(res6)

      case "nonLiteMat_q_3" =>
        rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>").
          union(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>")).
          union(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>"))
          .select("sDefault", "oDefault")

      case "nonLiteMat_q_4" =>
        val name = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>").withColumnRenamed("oDefault","n")
        val res1 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res2 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res3 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res4 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res5 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res6 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res7 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res8 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        val res9 = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>").withColumnRenamed("oDefault", "professor").
          join(rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>"), Seq("sDefault")).
          join(name, Seq("sDefault")).select("oDefault", "n")
        res1.union(res2).union(res3).union(res4).union(res5).union(res6).union(res7).union(res8).union(res9)

      case "nonLiteMat_q_5" =>
        val advs = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>")
        val nmst = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>").withColumnRenamed("oDefault","ns")
        val nmpr = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>").withColumnRenamed("oDefault","nx")
        val assiP = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>").withColumnRenamed("oDefault","professor")
        val assoP = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>").withColumnRenamed("oDefault","professor")
        val fullP = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>").withColumnRenamed("oDefault","professor")
        val undeS = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent>")
        val gradS = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
          rawDataFrame("oDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")
        val mbof = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>").withColumnRenamed("oDefault", "memberOf")
        val mkfr = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>").withColumnRenamed("oDefault", "memberOf")
        val hdof = rawDataFrame.where(rawDataFrame("pDefault") <=> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>").withColumnRenamed("oDefault", "memberOf")

        val res1 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(mbof, Seq("sDefault")).select("ns","nx","memberOf")
        val res2 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(mbof, Seq("sDefault")).select("ns","nx","memberOf")
        val res3 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(mbof, Seq("sDefault")).select("ns","nx","memberOf")
        val res4 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(mbof, Seq("sDefault")).select("ns","nx","memberOf")
        val res5 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(mbof, Seq("sDefault")).select("ns","nx","memberOf")
        val res6 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(mbof, Seq("sDefault")).select("ns","nx","memberOf")

        val res7 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(mkfr, Seq("sDefault")).select("ns","nx","memberOf")
        val res8 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(mkfr, Seq("sDefault")).select("ns","nx","memberOf")
        val res9 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(mkfr, Seq("sDefault")).select("ns","nx","memberOf")
        val res10 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(mkfr, Seq("sDefault")).select("ns","nx","memberOf")
        val res11 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(mkfr, Seq("sDefault")).select("ns","nx","memberOf")
        val res12 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(mkfr, Seq("sDefault")).select("ns","nx","memberOf")

        val res13 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(hdof, Seq("sDefault")).select("ns","nx","memberOf")
        val res14 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(hdof, Seq("sDefault")).select("ns","nx","memberOf")
        val res15 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(undeS, Seq("sDefault")).join(hdof, Seq("sDefault")).select("ns","nx","memberOf")
        val res16 = assiP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(hdof, Seq("sDefault")).select("ns","nx","memberOf")
        val res17 = assoP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(hdof, Seq("sDefault")).select("ns","nx","memberOf")
        val res18 = fullP.join(nmpr, Seq("sDefault")).withColumnRenamed("sDefault","oDefault").join(advs, Seq("oDefault")).join(nmst, Seq("sDefault")).join(gradS, Seq("sDefault")).join(hdof, Seq("sDefault")).select("ns","nx","memberOf")
        res1.union(res2).union(res3).union(res4).union(res5).union(res6).
          union(res7).union(res8).union(res9).union(res10).union(res11).union(res12).
          union(res13).union(res14).union(res15).union(res16).union(res17).union(res18)
    }
  }
}



object SparqlQueryInitializer extends QueryInitializer {

  override def initializeQueryStr(queryId: String = "eval_0"): String = queryId match {

    ////////////////////////
    // Eval Queries
    ////////////////////////

    case "eval_0" =>
      "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
      "select ?s " +
        " { " +
        " ?s <http://data.nasa.gov/qudt/owl/qudt/numericValue> \"0.32\"^^xsd:double . " +
        "} "

    case "eval_1" =>
      "select ?s ?o1 " +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ." +
        "} "

    case "eval_2" =>
      "select ?s ?o1 ?o2 ?o3 ?o4 ?o5" +
        " { " +
        " { " +
        "  ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "     <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
        "     <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 . " +
        " } " +
        " UNION " +
        "  { " +
        "  ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 ; " +
        "      <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "      <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o5 . " +
        " } " +
        " } "

    case "eval_3" =>
      "select ?s ?o1 ?o2 ?o3 ?o4 ?o5 ?o6 " +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 .  " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 . " +
        " } "

    case "eval_4" =>
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
        "select ?s ?o1 ?o2 ?o3" +
        " { " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o3 . " +
        " ?o1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cuahsi.org/waterML/flow> . " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cuahsi.org/waterML/temperature> . " +
        " ?o3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cuahsi.org/waterML/chlorine> . " +
        "} "

    case "eval_5" =>
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
        "select ?o11 ?o21 ?o31" +
        " { " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o3 . " +
        " ?o1 rdf:type <http://www.cuahsi.org/waterML/flow> ; " +
        "     <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o11 . " +
        " ?o2 rdf:type <http://www.cuahsi.org/waterML/temperature> ; " +
        "     <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o21 . " +
        " ?o3 rdf:type <http://www.cuahsi.org/waterML/chlorine> ; " +
        "     <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o31 . " +
        "}"


    ////////////////////////
    // Test Queries group 1
    ////////////////////////
//    case "test_0" =>
//      "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
//        " select ?s " +
//        "{ ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o ." +
//        "  ?o <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o1. " +
//        "  filter((?o > \"32\"^^xsd:double) &&  " +
//        "(?o1 < \"35\"^^xsd:double)) }"

    case "test_0" =>
      "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
        "select ?s ?o1 ?o2 ?o3 ?o4 ?o5 ?o6 " +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 .  " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 . " +
        " filter((?o6 > 2 + \"32\"^^xsd:double)) " +
        "} "


    case "test_1" =>
      "PREFIX val: <http://purl.oclc.org/NET/ssnx/ssn/> " +
        "PREFIX ty: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        "select distinct ?s ?o1" +
        " where { " +
        " {?s  val:hasValue ?o .}" +
        " {?o  ty:type ?o1 .} " +
        " } " +
        "GROUP BY ?s ?o1"

    case "test_2" =>
      "select ?s ?o1 ?o2 ?o3 ?o4 ?o5 ?o6 " +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 .  " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 . " +
        " } "

    case "test_3" =>
      "select ?s ?o6" +
        " { " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 .  " +
        " ?o2 <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 ; " +
        "     <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 . " +
        " ?s  <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 . " +
        " ?o2 <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 . " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o . " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 . " +
        " } "

    case "test_4" =>
      "select ?p " +
        "where { " +
        " ?p1 ?aa ?c1 . " +
        " ?p1 ?bb ?c2 . " +
        " ?p2 ?bb ?c3 . " +
        " ?p2 ?cc ?c4 . " +
        " ?c1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_village>; " +
        "     <http://test/locatedIn> ?w." +
        " ?c2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_site>; " +
        "     <http://test/locatedIn> ?x." +
        " ?c3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_city>; " +
        "     <http://test/locatedIn> ?y. " +
        " ?c4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_region>;" +
        "     <http://test/locatedIn> ?z. " +
        " } "


    case "test_5" =>
      "select ?a " +
        " { " +
        " ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_actor>; " +
        "    <http://test/livesIn> ?city; " +
        "    <http://test/actedIn> ?m1. " +
        " ?m1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_movie>." +
        " ?a <http://test/directed> ?m2." +
        " ?m2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_movie>." +
        " } "

    case "test_6" =>
      "select ?p " +
        "where { " +
        " ?p ?ss ?c1 . " +
        " ?p ?dd ?c2 ." +
        " ?c1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_village>; " +
        "     <http://test/locatedIn> ?x." +
        " ?c2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/wordnet_site>;" +
        "     <http://test/locatedIn> ?y." +
        " } "

    case "test_7" =>
      "select ?s ?o1 ?o2 ?o3 ?o4 ?o5 ?o6" +
        " {{ " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 .  }" +
        "UNION " +
        " {?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 .} " +
        " } "

    case "test_8" =>
      "construct{ " +
        "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o." +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o1. " +
        " ?s <http://left-join-test> ?o2 ." +
        "}" +
        //        "select *" +
        " where { " +
        "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o; " +
        " <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o1. " +
        "OPTIONAL { ?s <http://left-join-test> ?o2 .}" +
        "}"


    ////////////////////////
    // Construct-type Queries
    ////////////////////////
    case "test_9" =>
      "construct{ " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o." +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1. " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 " +
        "}" +
        " where { " +
        "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o; " +
        "   <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1; " +
        "   <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 ." +
        "}"

    case "test_10" =>
      "construct{ " +
        " ?sDefault <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?oDefault." +
        "}" +
        " where { " +
        "?sDefault <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?oDefault; " +
        "}"

    case "test_eval_4" =>
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
        "select ?s ?o1 ?o2 ?o3" +
        " { " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o1 . " +
        " ?o1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cuahsi.org/waterML/flow> . " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 . " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cuahsi.org/waterML/temperature> . " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o3 . " +
        " ?o3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cuahsi.org/waterML/chlorine> . " +
        "} "

    /////////////////////////////////////
    // Test Queries group 2 (Aggregation)
    /////////////////////////////////////
    case "test_agg_1" =>
      "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
        "select (min(?o3) as ?minMeasurement ) (max(?o2) as ?maxMeasurement)" +
        " { " +
        " ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o1 .  " +
        " ?o1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o2 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o3 . " +
        "}" +
        "group by ?s "

    //////////////////////////////////////////////////
    // Test Queries group 3 LiteMat_Concept_Properties
    //////////////////////////////////////////////////
    case "test_litemat_1" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?x " +
        " WHERE { " +
        " ?x rdf:type ub:Student " +
        " }"

    case "test_litemat_2" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?x " +
        " WHERE {" +
        "?x rdf:type ub:Professor. " +
        " }"

    case "test_litemat_3" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?x ?y " +
        " WHERE { " +
        "   ?x ub:worksFor ?y. " +
        " } "

    case "test_litemat_4" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?x ?y " +
        " WHERE {" +
        " ?x rdf:type ub:Professor; " +
        "    ub:worksFor ?y. " +
        " }"

    case "test_litemat_4_1" => "" +
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
      " PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
      " SELECT ?x ?y " +
      " WHERE { " +
      "{ ?x rdf:type ub:AssistantProfessor ; " +
      "     ub:headOf ?y . } UNION" +
      "{ ?x rdf:type ub:AssociateProfessor ; " +
      "     ub:headOf ?y . } UNION " +
      "{ ?x rdf:type ub:Chair ; " +
      "     ub:headOf ?y . } UNION " +
      "{ ?x rdf:type ub:FullProfessor ; " +
      "     ub:headOf ?y . } UNION" +
      "{ ?x rdf:type ub:Dean ; " +
      "     ub:headOf ?y . } UNION " +
      "{ ?x rdf:type ub:VisitingProfessor; " +
      "     ub:headOf ?y . } UNION " +
      "{ ?x rdf:type ub:AssistantProfessor ; " +
      "     ub:worksFor ?y . } UNION" +
      "{ ?x rdf:type ub:AssociateProfessor ; " +
      "     ub:worksFor ?y . } UNION " +
      "{ ?x rdf:type ub:Chair ; " +
      "     ub:worksFor ?y . } UNION " +
      "{ ?x rdf:type ub:FullProfessor ; " +
      "     ub:worksFor ?y . } UNION " +
      "{ ?x rdf:type ub:Dean ; " +
      "     ub:worksFor ?y . } UNION " +
      "{ ?x rdf:type ub:VisitingProfessor ; " +
      "     ub:worksFor ?y . } " +
      "}"

    //////////////////////////////////////
    // Test Queries: group 4 LiteMat_sameAS
    //////////////////////////////////////
    case "group_4_q_1" =>
     " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
       " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
       " SELECT ?n " +
       " WHERE { " +
       " ?x rdf:type lubm:Professor; " +
       "    lubm:name ?n . " +
       " } "

    case "group_4_q_2" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?ns ?nx " +
        " WHERE { " +
        " ?x rdf:type lubm:Professor ;" +
        "    lubm:name ?nx ." +
        " ?s lubm:advisor ?x ;" +
        "    rdf:type lubm:Student ;" +
        "    lubm:name ?ns . " +
        "}"

    case "group_4_q_3" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?x ?o " +
        " WHERE { " +
        " ?x lubm:memberOf ?o ." +
        "}"

    case "group_4_q_4" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?o ?n " +
        " WHERE { " +
        " ?x rdf:type lubm:Professor ;" +
        "    lubm:memberOf ?o ; " +
        "    lubm:name ?n ." +
        "}"

    case "group_4_q_5" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?ns ?nx ?o " +
        " WHERE { " +
        " { ?x rdf:type lubm:Professor ; " +
        "    lubm:name ?nx ;" +
        "    lubm:memberOf ?o . }" +
        " { ?s lubm:advisor ?x ; " +
        "      rdf:type lubm:Student ;" +
        "      lubm:name ?ns . } " +
        "}"

    case "group_4_q_6" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?n ?e " +
        " WHERE {  " +
        " ?x rdf:type lubm:PostDoc ;  " +
        "    lubm:name ?n ; " +
        "    lubm:email ?e ." +
        "} "

    case "group_4_q_7" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?o ?n " +
        " WHERE {  " +
        " ?x rdf:type lubm:Faculty ;  " +
        "    lubm:memberOf ?o ;" +
        "    lubm:name ?n ." +
        "}"

    case "group_4_q_8" =>
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        " PREFIX lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
        " SELECT ?ns ?nx ?o " +
        " WHERE {  " +
        " { ?x rdf:type lubm:Faculty ;  " +
        "      lubm:name ?nx ; " +
        "      lubm:memberOf ?o . } " +
        " { ?s  lubm:advisor ?x ;  " +
        "      rdf:type lubm:Student ;  " +
        "      lubm:name ?ns .  } " +
        "}"



  }
}