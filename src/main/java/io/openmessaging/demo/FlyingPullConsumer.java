package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.fly.store.FlyingStore;
import io.openmessaging.fly.store.GetMessageResult;
import io.openmessaging.fly.store.MessageDecoder;
import io.openmessaging.fly.store.SelectMapedBufferResult;

public class FlyingPullConsumer implements PullConsumer {
	
	/** CommitLog每个文件大小 1G */
	private static final int mapedFileSizeCommitLog = 1024 * 1024 * 1024;
	
	/** 最大消息大小，默认32K */
	private static final int maxMessageSize = 1024 * 32;

	/** ConsumeQueue刷盘间隔时间（单位毫秒） */
	private static final int flushIntervalConsumeQueue = 1000;

	/** 刷ConsumeQueue，至少刷几个PAGE */
	private static final int flushConsumeQueueLeastPages = 2;

	/** 刷ConsumeQueue，彻底刷盘间隔时间 */
	private static final int flushConsumeQueueThoroughInterval = 1000 * 60;

	private FlyingStore messageStore;

	private KeyValue properties;

	private String queue;

	private Set<String> buckets = new HashSet<>();

	private long offset = 0L;

	public FlyingPullConsumer(KeyValue properties) {
		this.properties = properties;
		this.properties.put("MAPED_FILE_SIZE", mapedFileSizeCommitLog);
		this.properties.put("MAX_MESSAGE_SIZE", maxMessageSize);
		this.properties.put("FLUSH_INTERVAL_CONSUME_QUEUE", flushIntervalConsumeQueue);
		this.properties.put("FLUSH_CONSUME_QUEUE_LEASTPAGES", flushConsumeQueueLeastPages);
		this.properties.put("FLUSH_CONSUME_QUEUE_THOROUGHINTERVAL", flushConsumeQueueThoroughInterval);
		messageStore = new FlyingStore(properties);
		messageStore.load();
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	@Override
	public synchronized Message poll() {
		String topic = buckets.iterator().next();
		GetMessageResult result = messageStore.getMessage(topic, queue, offset);
		if (result == null) {
			return null;
		}
		if (result.getMessageList() == null) {
			return null;
		}
		if (result.getMessageList().size() == 0) {
			return null;
		}
		offset = result.getNextBeginOffset();
		SelectMapedBufferResult smbr = (SelectMapedBufferResult) result.getMessageList().get(0);
		ByteBuffer buffer = smbr.getByteBuffer();
		BytesMessage bytesMessage = MessageDecoder.decode(buffer);
		return bytesMessage;
	}

	@Override
	public synchronized void attachQueue(String queueName, Collection<String> topics) {
		if (queue != null && !queue.equals(queueName)) {
			throw new ClientOMSException("You have alreadly attached to a queue " + queue);
		}

		queue = queueName;
		buckets.addAll(topics);
	}

	@Override
	public Message poll(KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

}
