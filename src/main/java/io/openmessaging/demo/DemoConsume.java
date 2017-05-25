package io.openmessaging.demo;

import java.util.Collections;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

public class DemoConsume {

	private static final String path = "/Users/hejie/flyingmq";

	public static void main(String[] args) {
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				long startConsumer = System.currentTimeMillis();
				KeyValue properties = new DefaultKeyValue();
				properties.put("STORE_PATH", path);
				String topic1 = "TOPIC1";
				// 消费样例1，实际测试时会Kill掉发送进程，另取进程进行消费
				PullConsumer consumer = new FlyingPullConsumer(properties);
				consumer.attachQueue("9", Collections.singletonList(topic1));
				while (true) {
					Message message = consumer.poll();
					if (message == null) {
						// 拉取为null则认为消息已经拉取完毕
						break;
					}
					//String topic = message.headers().getString(MessageHeader.TOPIC);
					//System.err.println(topic + ":" + new String(((BytesMessage) message).getBody()));
				}
				long endConsumer = System.currentTimeMillis();
				long T2 = endConsumer - startConsumer;
				System.out.println(String.format("Team2 cost:%d ms", T2) + String.format(", %d s", T2 / 1000));
			}
		});
		
		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				KeyValue properties = new DefaultKeyValue();
				properties.put("STORE_PATH", path);
				String topic1 = "TOPIC2";
				// 消费样例1，实际测试时会Kill掉发送进程，另取进程进行消费
				PullConsumer consumer = new FlyingPullConsumer(properties);
				consumer.attachQueue("0", Collections.singletonList(topic1));
				while (true) {
					Message message = consumer.poll();
					if (message == null) {
						// 拉取为null则认为消息已经拉取完毕
						break;
					}
					String topic = message.headers().getString(MessageHeader.TOPIC);
					System.err.println(topic + ":" + new String(((BytesMessage) message).getBody()));
				}
			}
		});
		t1.start();
		//t2.start();
	}
}
