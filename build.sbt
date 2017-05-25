name := "strider"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.10" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.0.2",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.2",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.2",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.2",
  "org.apache.jena" % "jena-core" % "3.0.1",
  "org.apache.jena" % "jena-arq" % "3.0.1"
)


val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case x if x.endsWith(".class") => MergeStrategy.last
  case x if x.endsWith(".properties") => MergeStrategy.last
  case x if x.contains("/resources/") => MergeStrategy.last
  case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
  case x if x.startsWith("META-INF/mimetypes.default") => MergeStrategy.first
  case x if x.startsWith("META-INF/maven/org.slf4j/slf4j-api/pom.") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    if (oldStrategy == MergeStrategy.deduplicate)
      MergeStrategy.first
    else
      oldStrategy(x)
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
    _.data.getName == "avro-ipc-1.7.7-tests.jar"
  }
}
