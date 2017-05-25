package io.openmessaging.fly.store;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;
import java.util.logging.Logger;

public class DefaultAppendMessageCallback {
	
	public final static Logger LOG = Logger.getLogger(DefaultAppendMessageCallback.class.getName());
	
	/** 文件结束码 cbd43194 */
	public final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;

	/** 编码格式 */
	public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

	/** 消息尾最小空白长度 */
	public final static int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

	public final static String ENTRY_SPLIT = "" + (char) 1;

	public final static String KEY_VALUE_SPLIT = "" + (char) 2;

	/** 消息魔术码 daa320a7 */
	public final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;

	/** 消息ID长度 */
	public final static int MSG_ID_LENGTH = 8 + 8;

	/**
	 * 计算消息的大小.
	 * 
	 * @param bodyLength
	 * @param topicLength
	 * @param propertiesLength
	 * @param contextLength
	 * @return
	 */
	public static int calMessageLength(int bodyLength, int topicLength, int propertiesLength, int contextLength) {
		return (4 // 1 TOTALSIZE
				+ 4 // 2 MAGICCODE
				+ 4 // 3 BODYCRC
				+ 4 // 4 QUEUEID
				+ 8 // 5 QUEUEOFFSET
				+ 8 // 6 PHYSICALOFFSET
				// + 4 // 7 SYSFLAG
				// + 8 // 8 STORETIMESTAMP
				+ 8 // 9 STOREHOSTADDRESS
				// + 4 // 10 RECONSUMETIMES
				+ 4 + (bodyLength > 0 ? bodyLength : 0) // 11 BODY
				+ 1 + topicLength // 12 TOPIC
				+ 2 + (propertiesLength > 0 ? propertiesLength : 0) // 13
																	// PROPERTY
				+ 2 + (contextLength > 0 ? contextLength : 0)); // 14 CONTEXT
	}

	/**
	 * 创建消息ID
	 *
	 * @param byteBuffer
	 * @param addr
	 * @param offset
	 * @return
	 */
	public static String createMessageId(ByteBuffer byteBuffer, byte[] addr, long offset) {
		byteBuffer.flip();
		byteBuffer.limit(MSG_ID_LENGTH);

		// 消息存储主机地址 IP PORT 8
		byteBuffer.put(addr);
		// 消息对应的物理分区 OFFSET 8
		byteBuffer.putLong(offset);

		return UtilAll.bytes2string(byteBuffer.array());
	}

	/**
	 * 将MAP转化为string
	 * 
	 * @param map
	 * @return MAP转化为string
	 */
	public static String map2String(Map<String, String> map) {
		return map2String(map, ENTRY_SPLIT, KEY_VALUE_SPLIT);
	}

	/**
	 * 将MAP转化为string
	 * 
	 * @param map
	 * @param entrySplit
	 * @param keyValueSplit
	 * @return MAP转化为string
	 */
	public static String map2String(Map<String, String> map, String entrySplit, String keyValueSplit) {
		if (map == null || map.isEmpty()) {
			return "";
		}

		StringBuilder mapStringBuilder = new StringBuilder();
		for (Map.Entry<String, String> keyValue : map.entrySet()) {
			mapStringBuilder.append(keyValue.getKey());
			mapStringBuilder.append(keyValueSplit);
			mapStringBuilder.append(keyValue.getValue());
			mapStringBuilder.append(entrySplit);
		}

		String contentWithLastSplit = mapStringBuilder.toString();
		if (null == contentWithLastSplit || "".equals(contentWithLastSplit)) {
			return null;
		} else {
			return contentWithLastSplit.substring(0, contentWithLastSplit.length() - entrySplit.length());
		}
	}

	/** 最大消息大小 */
	private final int maxMessageSize;

	/** 消息ID缓存 */
	//private final ByteBuffer msgIdMemory;

	/** 消息内容缓存 */
	private final ByteBuffer msgStoreItemMemory;

	/** 逻辑队列OFFSET key:topic-queueid */
	private HashMap<String, Long> topicQueueTable = new HashMap<String, Long>(1024);

	/**
	 * @param 最大消息大小
	 */
	DefaultAppendMessageCallback(final int size) {
		//this.msgIdMemory = ByteBuffer.allocate(MSG_ID_LENGTH);
		this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
		this.maxMessageSize = size;
	}

	/**
	 * 
	 * @param fileFromOffset
	 * @param byteBuffer
	 * @param maxBlank
	 * @param value
	 * @return
	 */
	public PutMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, Object value) {
		long createTime = System.currentTimeMillis();
		long eclipseTimeInLock = 0;
		long beginLockTimestamp = System.currentTimeMillis();
		BytesMessage msg = (BytesMessage) value;

		// 计算各字段长度
		short propertiesLength = 0;
		short contextLength = 0;
		byte[] topicData = msg.headers().getString(MessageHeader.TOPIC).getBytes(CHARSET_UTF8);
		int topicLength = topicData == null ? 0 : topicData.length;
		int bodyLength = msg.getBody() == null ? 0 : msg.getBody().length;
		int msgLen = calMessageLength(bodyLength, topicLength, propertiesLength, contextLength);

		// 长度验证
		if (msgLen > this.maxMessageSize) {
			LOG.warning("message size exceeded, msg total size: " + msgLen + ", msg body size:" + bodyLength + ", maxMessageSize:" + this.maxMessageSize);
			return new PutMessageResult(PutMessageStatus.MESSAGE_SIZE_EXCEEDED, null);
		}

		// 计算当前文件是否有足够空间
		if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
			this.resetMsgStoreItemMemory(maxBlank);
			// 1 TOTALSIZE
			this.msgStoreItemMemory.putInt(maxBlank);
			// 2 MAGICCODE
			this.msgStoreItemMemory.putInt(BlankMagicCode);
			// 3 The remaining space may be any value
			//
			// Here the length of the specially set maxBlank
			byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
			return new PutMessageResult(PutMessageStatus.END_OF_FILE, new AppendMessageResult(msg.headers().getString(MessageHeader.QUEUE), 0, maxBlank, null, createTime, 0));
		}

		// 计算物理OFFSET
		long wroteOffset = fileFromOffset + byteBuffer.position();

		String msgId = "";
		//String msgId = createMessageId(this.msgIdMemory, UtilAll.getBrokerHost(), wroteOffset);
		//msg.putHeaders(MessageHeader.MESSAGE_ID, msgId);

		eclipseTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
		if (eclipseTimeInLock > 100) {
			LOG.warning("[NOTIFYME]get msgId time(ms)=" + eclipseTimeInLock);
		}

		// 计算队列OFFSET
		String key = msg.headers().getString(MessageHeader.TOPIC) + "-" + msg.headers().getString(MessageHeader.QUEUE);
		Long queueOffset = topicQueueTable.get(key);
		if (null == queueOffset) {
			queueOffset = 0L;
			topicQueueTable.put(key, queueOffset);
		}

		eclipseTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
		if (eclipseTimeInLock > 500) {
			LOG.warning("[NOTIFYME]sufficient free space check time(ms)=" + eclipseTimeInLock);
		}

		// Initialization of storage space
		this.resetMsgStoreItemMemory(msgLen);
		// 1 TOTALSIZE
		this.msgStoreItemMemory.putInt(msgLen);
		// 2 MAGICCODE
		this.msgStoreItemMemory.putInt(MessageMagicCode);
		// 3 BODYCRC
		this.msgStoreItemMemory.putInt(UtilAll.crc32(msg.getBody()));
		// 4 QUEUEID
		Long queueId = msg.properties().getLong("queueId");
		//this.msgStoreItemMemory.putInt(Integer.parseInt(msg.headers().getString(MessageHeader.QUEUE)));
		this.msgStoreItemMemory.putInt(queueId.intValue());
		//byte[] queueData = msg.headers().getString(MessageHeader.QUEUE).getBytes(CHARSET_UTF8);
		//int queueId = queueData.hashCode();
		//this.msgStoreItemMemory.putInt(queueId);
		// 5 QUEUEOFFSET
		this.msgStoreItemMemory.putLong(queueOffset);
		// 6 PHYSICALOFFSET
		this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
		// 7 SYSFLAG
		// this.msgStoreItemMemory.putInt(msg.getSysFlag());
		// 8 STORETIMESTAMP
		// this.msgStoreItemMemory.putLong(msg.getCreateTime());
		// 9 STOREHOSTADDRESS
		this.msgStoreItemMemory.put(UtilAll.getBrokerHost());
		// 10 RECONSUMETIMES
		// this.msgStoreItemMemory.putInt(msg.getReconsumeTimes());
		// 11 TOPIC
		this.msgStoreItemMemory.put((byte) topicLength);
		this.msgStoreItemMemory.put(topicData);
		// 12 PROPERTIES
		// this.msgStoreItemMemory.putShort(propertiesLength);
		// if (propertiesLength > 0)
		// this.msgStoreItemMemory.put(propertiesData);
		// 13 CONTEXT
		// this.msgStoreItemMemory.putShort(contextLength);
		// if (contextLength > 0)
		// this.msgStoreItemMemory.put(contextData);
		// 14 BODY
		this.msgStoreItemMemory.putInt(bodyLength);
		if (bodyLength > 0)
			this.msgStoreItemMemory.put(msg.getBody());

		// Write messages to the queue buffer
		byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

		eclipseTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
		if (eclipseTimeInLock > 1000) {
			LOG.warning("[NOTIFYME]byte buffer put time(ms)=" + eclipseTimeInLock);
		}
		
		AppendMessageResult amr = new AppendMessageResult(String.valueOf(queueId), wroteOffset, msgLen, msgId, createTime, queueOffset);
		PutMessageResult result = new PutMessageResult(PutMessageStatus.PUT_OK, amr);
		
		topicQueueTable.put(key, ++queueOffset);
		
		if (amr.getQueueId() == null) {
			System.err.println(Thread.currentThread().getId() + "_" +queueId);
		}
		return result;
	}

	/**
	 * Getter method for property <tt>msgStoreItemMemory</tt>.
	 *
	 * @return property value of msgStoreItemMemory
	 */
	public ByteBuffer getMsgStoreItemMemory() {
		return msgStoreItemMemory;
	}

	/**
	 * 重置消息缓存
	 *
	 * @param length
	 */
	private void resetMsgStoreItemMemory(final int length) {
		this.msgStoreItemMemory.flip();
		this.msgStoreItemMemory.limit(length);
	}
}
