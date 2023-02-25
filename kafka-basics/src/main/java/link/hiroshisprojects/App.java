package link.hiroshisprojects;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Progammatic implementations of a Kafka producer and consumer to a Kafka cluster running locally. 
 */
public class App {

	private static final Logger LOGGER = LoggerFactory.getLogger(App.class.getSimpleName());

	public static void main(String[] args) {
		consumerDemo();
	}

	static void consumerDemo() {
		LOGGER.info("I am a Kafka Consumer");

		// topics are logical groupings of events, analogous to tables in a relational db
		String topic = "demo_java";

		// properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		// we're assuming the consumed data can be deserialized to string key-value pairs. A registry helps communicate the data "type" to consumers
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// defines consumer group this consumer is a part of
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-java-application");

		// read from the beginning of the partition
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// KafkaConsumer is a Closeable object 
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){

			// get a reference to the main thread so we can call .join() on it and prevent an ungraceful shutdown
			final Thread mainThread = Thread.currentThread();

			// shutdown hook if a shutdown attempt is made during polling
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					LOGGER.info("Detected a shutdown. Let's exit by calling consumer.wakeup()...");

					// toggles a flag that causes consumer.poll to throw WakeupException, prompting loop to end
					consumer.wakeup();
					
					try {
						// return to main thread so it can clean up resources
						mainThread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});

			consumer.subscribe(Arrays.asList(topic));

			while(true) {
				LOGGER.info("Polling");

				// argument defines how long the consumer "listens" for records per poll
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				// log contents of poll
				for (ConsumerRecord<String, String> record: records) {
					LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
					LOGGER.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				}
			}
		} catch (WakeupException e) {
			LOGGER.info("Poll closed.");
		}	catch (Exception e) {
			LOGGER.error("Unexpected exception in consumer" , e);
		}
		
	}

	void producerDemo() {
		LOGGER.info("I am a Kafka Producer");

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++) {
			String topic = "demo_java";
			String key = "id_" + i;
			String value = "ohayo gozaimasu " + i;

			ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

			// send record and confirm successful transmission 
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e == null) {
						LOGGER.info("Received new metadata \n" + 
						"\tKey: " + key + "\n" +  
						"\tPartition: " + metadata.partition() + "\n");
					}
				}
			});

		}

		// flushing sends all 10 records at once
		producer.flush();

		producer.close();

	}
}



