package engine.launcher.streamgenerator

import engine.stream.{KafkaStreamProducer, MsgRDFTriple, RDFTriple}
import kafka.producer.{Producer, ProducerConfig}

import scala.util.Random

/**
  * Created by xiangnanren on 10/12/2016.
  */
case class Generator2(workLoad: Long,
                      sleepDuration: Long) extends StreamGenerator {
  val kafkaStreamProducer = KafkaStreamProducer(
    "localhost:9092", "", "", None, MsgRDFTriple.value)

  val producer = new Producer[String, RDFTriple](
    new ProducerConfig(kafkaStreamProducer.kafkaParamProps))

  def randomNumeric(): Double = {
    val min: Int = 10
    val max: Int = 60
    (Random.nextInt(max) % (max - min + 1) + min).toDouble / 100
  }

  def randomProportion(): Int = {
    val upperBound: Int = 10
    val lowerBound: Int = 1
    val rnd = new scala.util.Random
    lowerBound + rnd.nextInt((upperBound - lowerBound) + 1)
  }

  override def generate(): Unit = {
    var messageId: Long = 0
    var numTypeFlow: Int = 500
    var numTypeTemperature: Int = 100
    var numTypeChlorine: Int = 100

    val f_unitWorkLoad: (Int, Int, Int) => Int =
      (numTypeFlow: Int,
       numTypeTemperature: Int,
       numTypeChlorine: Int
      ) => {
        numTypeFlow +
          numTypeTemperature +
          numTypeChlorine
      }

    // Send message triple by triple
    while (messageId *
      f_unitWorkLoad(
        numTypeFlow,
        numTypeTemperature,
        numTypeChlorine) < workLoad) {

      ///////////////////////////////////////// flow type

      (1 to numTypeFlow).foreach(flowId => {
        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/stream/1i/_" + messageId + ">",
            "<http://purl.oclc.org/NET/ssnx/ssn/hasValue>",
            "<http://ontology.waves.org/Obs_a" + messageId + "_" + flowId + ">"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Obs_a" + messageId + "_" + flowId + ">",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://www.cuahsi.org/waterML/flow>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Obs_a" + messageId + "_" + flowId + ">",
            "<http://data.nasa.gov/qudt/owl/qudt/numericValue>",
            "\"" + (randomNumeric(): Double).toString + "\"" +
              "^^<http://www.w3.org/2001/XMLSchema#double>"))
      })

      ///////////////////////////////////////// temperature type

      (1 to numTypeTemperature).foreach(temperatureId => {
        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/stream/1i/_" + messageId + ">",
            "<http://purl.oclc.org/NET/ssnx/ssn/hasValue>",
            "<http://ontology.waves.org/Obs_b" + messageId + "_" + temperatureId + ">"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Obs_b" + messageId + "_" + temperatureId + ">",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://www.cuahsi.org/waterML/temperature>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Obs_b" + messageId + "_" + temperatureId + ">",
            "<http://data.nasa.gov/qudt/owl/qudt/numericValue>",
            "\"" + (randomNumeric(): Double).toString + "\"" +
              "^^<http://www.w3.org/2001/XMLSchema#double>"))
      })

      ///////////////////////////////////////// chlorine type
      (1 to numTypeChlorine).foreach(chlorineId => {

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/stream/1i/_" + messageId + ">",
            "<http://purl.oclc.org/NET/ssnx/ssn/hasValue>",
            "<http://ontology.waves.org/Obs_c" + messageId + "_" + chlorineId + ">"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Obs_c" + messageId + "_" + chlorineId + ">",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://www.cuahsi.org/waterML/chlorine>"))

        kafkaStreamProducer.sendMessage(
          producer,
          new RDFTriple("<http://ontology.waves.org/Obs_c" + messageId + "_" + chlorineId + ">",
            "<http://data.nasa.gov/qudt/owl/qudt/numericValue>",
            "\"" + (randomNumeric(): Double).toString + "\"" +
              "^^<http://www.w3.org/2001/XMLSchema#double>"))
      })

      messageId += 1

      if (messageId * f_unitWorkLoad(
        numTypeFlow,
        numTypeTemperature,
        numTypeChlorine) >= workLoad) {
//
//        numTypeFlow = randomProportion()
//        numTypeTemperature = randomProportion()
//        numTypeChlorine = randomProportion()

        messageId = 0
        Thread.sleep(sleepDuration * 1000)
      }
    }
    producer.close()
  }
}
