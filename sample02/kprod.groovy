#!/usr/bin/env groovy

@Grab(group='org.apache.kafka', module='kafka-clients', version='2.0.0')

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class Kprod {
   
   static void main(args) throws Exception{

      //Assign topicName to string variable
      def topicName = "sample"
      
      if(! args.length == 0){
         println("Set topicName: $topicName")
      }

      def producer = new KafkaProducer( 
        [      
               'bootstrap.servers':'localhost:9092',
               'acks':'all',
               'retries':0,
               'batch.size': 16384,
               'linger.ms': 1,
               'buffer.memory': 33554432,
               'key.serializer': "org.apache.kafka.common.serialization.StringSerializer",
               'value.serializer': "org.apache.kafka.common.serialization.StringSerializer"
        ]
      )

      for(i in 0..9)
         producer.send(new ProducerRecord<String, String>(topicName,Integer.toString(i), Integer.toString(i)))
         println("Message sent successfully")
         producer.close()
   }
}
