package com.releaseinfo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;

public class Consumer {

	public static void main(String[] args) {
		if (args.length == 1) {
			BasicConfigurator.configure();
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("group.id", "test");
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList(args[0]));
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(10);
					for (ConsumerRecord<String, String> record : records) {
						System.out.println(record.toString());
					}
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			} finally {
				consumer.close();
			}
		}
		else {
			System.out.println("Please Provide Argument : topic_name ");
		}
	}
}
