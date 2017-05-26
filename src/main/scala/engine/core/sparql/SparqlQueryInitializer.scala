package engine.core.sparql

/**
  * Created by xiangnanren on 07/07/16.
  */
trait QueryInitializer {
  def initializeQueryStr(queryId: String): String
}


object SparqlQueryInitializer extends QueryInitializer {

  override def initializeQueryStr(queryId: String = "test_0"): String = queryId match {

    ////////////////////////
    // Eval Queries
    ////////////////////////

    case "eval_0" =>
      "select ?s ?o " +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
//        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 ." +
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
    // Test Queries
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
        " select ?s ?o1" +
        "{ ?s <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o ." +
        "  ?o <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o1. " +
        "  filter((?o1 = \"0.22\"^^xsd:double)) " +
        "}"

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

    ////////////////////////
    // Aggregation
    ////////////////////////
    case "test_agg_1" =>
      "select ?s (group_concat(?s; separator = \",\") as ?min_s) " +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ." +
        "} " +
        "GROUP BY ?s "


  }
}