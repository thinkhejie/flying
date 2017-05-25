package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.List;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;

public class DemoProducer {

	private static final String path = "/Users/hejie/flyingmq";

	public static void main(String[] args) {
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				String topic1 = "TOPIC1";
				KeyValue properties = new DefaultKeyValue();
				properties.put("STORE_PATH", path);
				Producer producer = new FlyingProducer(properties);
				int num = 10000 * 4000;
				//int num = 40000000;
				long startConsumer = System.currentTimeMillis();
				for (int i = 0; i < num; i++) {
					producer.send(producer.createBytesMessageToTopic(topic1, ("hello wolrd:" + i).getBytes()));
				}
				long endConsumer = System.currentTimeMillis();
				long T2 = endConsumer - startConsumer;
				System.out.println(String.format("Team1 cost:%d ms", T2) + String.format(", %d s", (T2 / 1000)));
			}
		});
		
		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				String topic2 = "TOPIC2";
				KeyValue properties = new DefaultKeyValue();
				properties.put("STORE_PATH", path);
				Producer producer = new FlyingProducer(properties);
				int num = 1000000;
				List<Message> messagesForTopic1 = new ArrayList<>(1);
				for (int i = 0; i < num; i++) {
					messagesForTopic1.add(producer.createBytesMessageToTopic(topic2, ("hello wolrd:" + i).getBytes()));
				}
				for (int i = 0; i < num; i++) {
					producer.send(messagesForTopic1.get(i));
				}
			}
		});
		
		t1.start();
		//t2.start();
		//long endConsumer = System.currentTimeMillis();
		//long T2 = endConsumer - startConsumer;
		//System.out.println(String.format("Team1 cost:%d ms", T2));
	}
}
