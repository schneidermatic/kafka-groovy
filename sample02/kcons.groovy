#!/usr/bin/env groovy

@Grab(group='org.apache.kafka', module='kafka-clients', version='2.0.0')

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.WakeupException
import groovy.transform.Synchronized

class Kcons {
   
   static void main(args) throws Exception {

      //Kafka consumer configuration settings
      String topicName = "sample"

      if(! args.length == 0){
         println("Set topicName: $topicName")
      }

      KafkaConsumer kafkaConsumer = new KafkaConsumer(
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

      def executorService = Executors.newFixedThreadPool(1)
      executorService.execute(new KafkaConsumerRunner(kafkaConsumer, "sample"))
   }
}


class KafkaConsumerRunner implements Runnable {

    AtomicBoolean closed = new AtomicBoolean(false)
    KafkaConsumer consumer
    String topicName

    KafkaConsumerRunner(KafkaConsumer consumer, String topicName) {
        this.consumer = consumer
        this.topicName = topicName
    }

    @Override
    public void run() {
        try {
            def records
            synchronized (consumer) {
                this.consumer.subscribe([topicName])
            }
            while (!closed.get()) {
                synchronized (consumer) {
                    records = consumer.poll(100)
                }
                for (record in records) {
                    // print the offset,key and value for the consumer records.
                    printf("offset = %d, key = %s, value = %s\n",record.offset(), record.key(), record.value())
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            System.out.println(e);
            //if (!closed.get()) throw e;
        }
    }

    // Shutdown hook which can be called from a separate thread
    void shutdown() {
        closed.set(true)
        consumer.wakeup()
    }
}