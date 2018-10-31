package com.releaseinfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;

public class Producer {
	public static void main(String[] args) {
		if (args.length == 2) {
			BasicConfigurator.configure();
			
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			KafkaProducer<String, String> sampleProducer = new KafkaProducer<String, String>(props);
			// ProducerRecord<String, String> record = new ProducerRecord<String,
			// String>(topicName, data);
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(args[0]));
				String line = br.readLine();
				while ((line = br.readLine()) != null) {
					
					sampleProducer.send(new ProducerRecord<String, String>(args[1], line));
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			sampleProducer.close();
		} else {
			System.out.println("Please Provide Two Argumnets : jsonFile and topic_name ");
		}

	}
}
