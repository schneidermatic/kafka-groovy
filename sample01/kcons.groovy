#!/usr/bin/env groovy

@Grab(group='org.apache.kafka', module='kafka-clients', version='2.0.0')

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord

class Kcons {
   
   static void main(args) throws Exception {

      //Kafka consumer configuration settings
      def topicName = "sample"

      if(! args.length == 0){
         println("Set topicName: $topicName");
      }     
      
      def consumer = new KafkaConsumer(
                  [ 
                   'bootstrap.servers': 'localhost:9092',
                   'group.id': "test",
                   'enable.auto.commit': "true",
                   'auto.commit.interval.ms': "1000",
                   'session.timeout.ms': "30000",
                   'key.deserializer':"org.apache.kafka.common.serialization.StringDeserializer",
                   'value.deserializer':"org.apache.kafka.common.serialization.StringDeserializer"
                  ]
      )
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe([topicName])
      
      //print the topic name
      println("Subscribed to topic " + topicName)
      
      while (true) {
         for (record in consumer.poll(100))
         // print the offset,key and value for the consumer records.
         printf("offset = %d, key = %s, value = %s\n",record.offset(), record.key(), record.value())
      }
   }
}
