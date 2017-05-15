package integrationtest.streamtest.generator

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Locale

import engine.launcher.streamgenerator.StreamGenerator
import engine.stream.{KafkaStreamProducer, MsgRDFTriple, RDFTriple}
import kafka.producer.{Producer, ProducerConfig}

import scala.util.Random

/**
  * Created by xiangnanren on 09/12/2016.
  */
case class Generator1(workLoad: Long,
                      sleepDuration: Long) extends StreamGenerator {

  val kafkaStreamProducer = KafkaStreamProducer(
    "localhost:9092", "", "", None, MsgRDFTriple.value)

  val producer = new Producer[String, RDFTriple](
    new ProducerConfig(kafkaStreamProducer.kafkaParamProps))

  def randomNum(): Double = {
    val min: Int = 10
    val max: Int = 60
    (Random.nextInt(max) % (max - min + 1) + min).toDouble / 100
  }

  def date(): String = {
    val dstr: String = "2001-07-03T12:08:56.235-0700"
    val format: DateFormat =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH)
    dstr
  }

  override def generate(): Unit = {
    while (true) {
      var obsId: Long = 0

      // Send message triple by triple
      while (obsId < workLoad / 7) {
        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Observation_" + obsId + ">",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://purl.oclc.org/NET/ssnx/ssn/ObservationValue>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Observation_" + obsId + ">",
            "<http://purl.oclc.org/NET/ssnx/ssn/startTime>",
            "\"" + date + "\"" + "^^<http://www.w3.org/2001/XMLSchema#dateTime>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Observation_" + obsId + ">",
            "<http://data.nasa.gov/qudt/owl/qudt/unit>",
            "<http://www.units.org/2016/unit#lh>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Observation_" + obsId + ">",
            "<http://data.nasa.gov/qudt/owl/qudt/numericValue>",
            "\"" + (randomNum(): Double).toString + "\"" +
              "^^<http://www.w3.org/2001/XMLSchema#double>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://localhost:80/waves/stream/1i/_" + obsId + ">",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://purl.oclc.org/NET/ssnx/ssn/SensorOutput>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://localhost:80/waves/stream/1i/_" + obsId + ">",
            "<http://purl.oclc.org/NET/ssnx/ssn/isProducedBy>",
            "<http://www.zone-waves.fr/2016/sensor#CPT_RESEAU_AVE_BURAGO_DI_MALGORA>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://localhost:80/waves/stream/1i/_" + obsId + ">",
            "<http://purl.oclc.org/NET/ssnx/ssn/hasValue>",
            "<http://ontology.waves.org/Observation_" + +obsId + ">"))

        obsId += 1

        if (obsId >= workLoad / 7) {
          obsId = 0
          Thread.sleep(sleepDuration * 1000)
        }
      }
      producer.close()
    }
  }
}
