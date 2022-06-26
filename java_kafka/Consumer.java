package java_kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

public class Consumer {

	private final org.slf4j.Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
	private String mBootstrapServer = "";
	private String mGroupId = "";
	private String mTopic = "";

	Consumer(String bootstrapServer, String groupId, String topic) {
		mBootstrapServer = bootstrapServer;
		mGroupId = groupId;
		mTopic = topic;
	}

	void run() {
		mLogger.info("Creting consumer thread");
		CountDownLatch latch = new CountDownLatch(1);

		ConsumerRunnable consumerRunnable = new ConsumerRunnable(mBootstrapServer, mGroupId, mTopic, latch);
		Thread thread = new Thread(consumerRunnable);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			mLogger.info("Caught shutdown hook");
			consumerRunnable.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			mLogger.info("Application has exited");
		}));
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private class ConsumerRunnable implements Runnable {
		private CountDownLatch mLatch;
		private KafkaConsumer<String, String> mConsumer;

		ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
			mLatch = latch;

			Properties props = consumerProps(bootstrapServer, groupId);
			mConsumer = new KafkaConsumer<>(props);
			mConsumer.subscribe(Collections.singletonList(topic));
		}

		private Properties consumerProps(String bootstrapServer, String groupId) {
			String deserializer = StringDeserializer.class.getName();
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);

			return properties;
		}

		@Override
		public void run() {
			try {
				do {
					ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						mLogger.info("Key: " + record.key() + ", Value: " + record.value());
						mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				} while (true);
			} catch (WakeupException e) {
				mLogger.info("Received shutdown signal!");
			} finally {
				mConsumer.close();
				mLatch.countDown();
			}
		}

		void shutdown() {
			mConsumer.wakeup();
		}
	}

}
