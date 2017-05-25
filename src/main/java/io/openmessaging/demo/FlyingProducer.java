package io.openmessaging.demo;

import java.net.InetSocketAddress;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;
import io.openmessaging.fly.store.FlyingStore;
import io.openmessaging.fly.store.UtilAll;

public class FlyingProducer implements Producer {

	/** CommitLog每个文件大小 1G */
	private static final int mapedFileSizeCommitLog = 1024 * 1024 * 1024 * 1;

	/** 最大消息大小，默认32K */
	private static final int maxMessageSize = 1024 * 32;

	private MessageFactory messageFactory = new DefaultMessageFactory();

	private FlyingStore messageStore;

	private KeyValue properties;

	public FlyingProducer(KeyValue properties) {
		this.properties = properties;
		this.properties.put("MAPED_FILE_SIZE", mapedFileSizeCommitLog);
		this.properties.put("MAX_MESSAGE_SIZE", maxMessageSize);
		UtilAll.setBrokerHost(new InetSocketAddress("127.0.0.1", 1234));
		messageStore = new FlyingStore(properties);
		messageStore.load();
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
		return messageFactory.createBytesMessageToQueue(queue, body);
	}

	@Override
	public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
		return messageFactory.createBytesMessageToTopic(topic, body);
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see io.openmessaging.Producer#send(io.openmessaging.Message)
	 */
	@Override
	public void send(Message message) {
		if (message == null) {
			throw new ClientOMSException("Message should not be null");
		}
		String topic = message.headers().getString(MessageHeader.TOPIC);
		String queue = message.headers().getString(MessageHeader.QUEUE);

		if ((topic == null && queue == null) || (topic != null && queue != null)) {
			throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
		}
		message.putProperties("queueId", Thread.currentThread().getId() % 20);
		messageStore.putMessage(topic != null ? topic : queue, message);
	}

	@Override
	public void send(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void shutdown() {
		messageStore.shutdown();
	}

	@Override
	public void start() {
		messageStore.start();
	}
}
