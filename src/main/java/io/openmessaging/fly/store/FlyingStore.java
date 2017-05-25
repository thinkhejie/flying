package io.openmessaging.fly.store;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

public class FlyingStore {
	/** 日志对象 */
	private static final Logger LOG = Logger.getLogger(FlyingStore.class.getName());

	/** CommitLog每个文件大小 1G */
	private static int mapedFileSizeCommitLog;

	private DefaultAppendMessageCallback callback;

	private CommitLogRun clr;

	private MappedFileQueue queue;

	private Thread thread;

	private String storePathRootDir;

	/** ConsumeQueue集合 KEY1:TOPIC KEY2:QUEUEID */
	private static ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>> consumeQueueTable;

	public FlyingStore(KeyValue keyValue) {
		int mapedFileSize = keyValue.getInt("MAPED_FILE_SIZE");
		int maxMessageSize = keyValue.getInt("MAX_MESSAGE_SIZE");
		storePathRootDir = keyValue.getString("STORE_PATH");
		FlyingStore.mapedFileSizeCommitLog = mapedFileSize;
		callback = new DefaultAppendMessageCallback(maxMessageSize);
		queue = new MappedFileQueue(keyValue.getString("STORE_PATH") + File.separator + "commitlog", mapedFileSize);
		clr = new CommitLogRun(queue);
		thread = new Thread(clr);
		consumeQueueTable = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>>(10);
	}

	/**
	 * 
	 * @param topic
	 * @param queueId
	 * @param offset
	 * @return
	 */
	public GetMessageResult getMessage(String topic, String queueId, long offset) {
		long nextBeginOffset = 0L;
		GetMessageResult getResult = new GetMessageResult();
		ConcurrentHashMap<String, ConsumeQueue> chm = consumeQueueTable.get(topic);
		ConsumeQueue cq = chm.get(queueId);
		SelectMapedBufferResult smbr = cq.getIndexBuffer(offset);
		if (smbr == null) {
			String status = "OFFSET_FOUND_NULL";
			nextBeginOffset = nextOffsetCorrection(offset, rollNextFile(offset));
			LOG.warning("consumer request topic: " + topic + ",offset: " + offset + ", but access logic queue failed.");
			getResult.setStatus(status);
			getResult.setNextBeginOffset(nextBeginOffset);
			return getResult;
		}
		int i = 0;
		if (i < smbr.getSize()) {
			long offsetPy = smbr.getByteBuffer().getLong();
			int sizePy = smbr.getByteBuffer().getInt();
			SelectMapedBufferResult selectResult = queue.getMessage(offsetPy, sizePy);
			getResult.addMessage(selectResult);
			i += Constant.CQStoreUnitSize;
		}
		nextBeginOffset = offset + (i / Constant.CQStoreUnitSize);
		getResult.setNextBeginOffset(nextBeginOffset);
		smbr.release();
		return getResult;
	}

	/**
	 * 查询消息
	 *
	 * @param serializingType
	 * @param topic
	 * @param consumeGroup
	 * @param queueId
	 * @param offset
	 * @param maxMsgNums
	 * @return
	 */
	public GetMessageResult getMessage(String topic, String queueId, long offset, int maxMsgNums) {
		// 当被过滤后，返回下一次开始的Offset
		long nextBeginOffset = offset;
		GetMessageResult getResult = new GetMessageResult();
		String status = "NO_MESSAGE_IN_QUEUE";
		// ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
		// long minOffset = consumeQueue.getMinOffsetInQuque();
		// long maxOffset = consumeQueue.getMaxOffsetInQuque();
		// SelectMapedBufferResult bufferConsumeQueue =
		// consumeQueue.getIndexBuffer(offset);
		SelectMapedBufferResult bufferConsumeQueue = new SelectMapedBufferResult(0L, null, 1111111, null);

		// 有个读写锁，所以只访问一次，避免锁开销影响性能
		final long maxOffsetPy = queue.getMaxOffset();
		if (bufferConsumeQueue != null) {
			try {
				long nextPhyFileStartOffset = Long.MIN_VALUE;
				int i = 0;
				for (; i < bufferConsumeQueue.getSize(); i += Constant.CQStoreUnitSize) {
					long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
					int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
					// bufferConsumeQueue.getByteBuffer().getLong();

					// 说明物理文件正在被删除
					if (nextPhyFileStartOffset != Long.MIN_VALUE) {
						if (offsetPy < nextPhyFileStartOffset)
							continue;
					}

					// 判断是否拉磁盘数据
					boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

					// 此批消息达到上限了
					if (isTheBatchFull(sizePy, maxMsgNums, getResult.getTotalSize(), getResult.getCount(), isInDisk)) {
						break;
					}

					// 必须在使用完之后release
					SelectMapedBufferResult selectResult = queue.getMessage(offsetPy, sizePy);
					if (selectResult != null) {
						getResult.addMessage(selectResult);
						status = "FOUND";
						nextPhyFileStartOffset = Long.MIN_VALUE;
					} else {
						if (0 == getResult.getTotalSize()) {
							status = "MESSAGE_WAS_REMOVING";
						}

						// 物理文件正在被删除，尝试跳过
						nextPhyFileStartOffset = this.rollNextFile(offsetPy);
					}
				}
				nextBeginOffset = offset + (i / Constant.CQStoreUnitSize);
			} finally {
				// 必须释放资源
				bufferConsumeQueue.release();
			}
		}
		getResult.setNextBeginOffset(nextBeginOffset);
		getResult.setStatus(status);
		return getResult;
	}

	public void load() {
		queue.load();

		// load Consume Queue
		this.loadConsumeQueue();

		// recover
		this.recoverConsumeQueue();
	}

	public synchronized void putMessage(String bucket, Message message) {
		MappedFile mappedFile = queue.getLastMapedFileWithLock();
		if (null == mappedFile || mappedFile.isFull()) {
			mappedFile = queue.getLastMapedFile();
		}
		PutMessageResult result = mappedFile.appendMessage(message, callback);
		AppendMessageResult aresult = result.getAppendMessageResult();
		switch (result.getPutMessageStatus()) {
		case END_OF_FILE:
			LOG.severe("create maped file2 start, topic: " + message.headers().getString(MessageHeader.TOPIC));
			// Create a new file, re-write the message
			mappedFile = queue.getLastMapedFile();
			if (null == mappedFile) {
				// warn and notify me
				LOG.severe("create maped file2 error, topic: " + message.headers().getString(MessageHeader.TOPIC));
				// beginTimeInLock = 0;
				// return new
				// PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED,
				// null);
				break;
			}
			result = mappedFile.appendMessage(message, callback);
			aresult = result.getAppendMessageResult();
		default:
			// beginTimeInLock = 0;
			// return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR,
			// null);
			break;
		}
		// clr.wakeup();

		// 向逻辑队列写消息
		String topic = bucket;
		String queueId = aresult.getQueueId();
		long wroteOffset = aresult.getWroteOffset();
		int size = aresult.getWroteBytes();
		long storeTimestamp = result.getAppendMessageResult().getStoreTimestamp();
		long logicOffset = aresult.getOffset();
		putMessagePostionInfo(topic, queueId, wroteOffset, size, storeTimestamp, logicOffset);
	}

	public void shutdown() {

	}

	public void start() {
		thread.start();
	}

	/**
	 * 获取逻辑队列存储路径
	 * 
	 * @param rootDir
	 * @return
	 */
	public static String getConsumeQueuePath(final String rootDir) {
		return rootDir + File.separator + "consumequeue";
	}

	/**
	 * 加载逻辑队列
	 * 
	 * @return
	 */
	private boolean loadConsumeQueue() {
		File dirLogic = new File(getConsumeQueuePath(storePathRootDir));
		File[] fileTopicList = dirLogic.listFiles();
		if (fileTopicList != null) {
			// TOPIC 遍历
			for (File fileTopic : fileTopicList) {
				String topic = fileTopic.getName();
				// TOPIC 下队列遍历
				File[] fileQueueIdList = fileTopic.listFiles();
				if (fileQueueIdList != null) {
					for (File fileQueueId : fileQueueIdList) {
						if (!".DS_Store".equals(fileQueueId.getName())) {
							String queueId = fileQueueId.getName();
							ConsumeQueue logic = new ConsumeQueue(topic, queueId, getConsumeQueuePath(storePathRootDir), Constant.mapedFileSizeConsumeQueue);
							this.putConsumeQueue(topic, queueId, logic);
							if (!logic.load()) {
								return false;
							}
						}
					}
				}
			}
		}

		LOG.info("load logics queue all over, OK");

		return true;
	}

	/**
	 * 根据topic和队列查找逻辑队列对象
	 * 
	 * @param topic
	 * @param queueId
	 * @return
	 */
	public ConsumeQueue findConsumeQueue(String topic, String queueId) {
		ConcurrentHashMap<String, ConsumeQueue> map = consumeQueueTable.get(topic);
		if (null == map) {
			ConcurrentHashMap<String, ConsumeQueue> newMap = new ConcurrentHashMap<String, ConsumeQueue>(128);
			ConcurrentHashMap<String, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
			if (oldMap != null) {
				map = oldMap;
			} else {
				map = newMap;
			}
		}
		
		if (queueId == null) {
			LOG.severe("findConsumeQueue.queueId:" + queueId + ",topic:" + topic + ".");
		}
		
		ConsumeQueue logic = map.get(queueId);
		if (null == logic) {
			ConsumeQueue newLogic = new ConsumeQueue(topic, queueId, getConsumeQueuePath(storePathRootDir), Constant.mapedFileSizeConsumeQueue);
			ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
			if (oldLogic != null) {
				logic = oldLogic;
			} else {
				logic = newLogic;
			}
		}

		return logic;
	}

	/**
	 * 获取下一个文件的起始offset
	 * 
	 * @param offset
	 * @return
	 */
	public long rollNextFile(final long offset) {
		int mapedFileSize = mapedFileSizeCommitLog;
		return (offset + mapedFileSize - offset % mapedFileSize);
	}

	/**
	 * 检查消息是否落在磁盘
	 * 
	 * @param offsetPy
	 * @param maxOffsetPy
	 * @return
	 */
	private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
		long memory = (long) (getTotalPhysicalMemorySize() * (Constant.accessMessageInMemoryMaxRatio / 100.0));
		return (maxOffsetPy - offsetPy) > memory;
	}

	/**
	 * 获取当前环境物理内存大小
	 * 
	 * @return
	 */
	public static long getTotalPhysicalMemorySize() {
		long physicalTotal = 1024 * 1024 * 1024 * 24;
		OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
		if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
			physicalTotal = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
		}

		return physicalTotal;
	}

	/**
	 * 纠正下一个offset
	 * 
	 * @param oldOffset
	 * @param newOffset
	 * @return
	 */
	private long nextOffsetCorrection(long oldOffset, long newOffset) {// NOPMD
		return newOffset;
	}

	/**
	 * 一批消息拉取是否到达上限
	 * 
	 * @param sizePy
	 * @param maxMsgNums
	 * @param bufferTotal
	 * @param messageTotal
	 * @param isInDisk
	 * @return
	 */
	private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {
		// 第一条消息可以不做限制
		if (0 == bufferTotal || 0 == messageTotal) {
			return false;
		}

		if (messageTotal >= maxMsgNums) {
			return true;
		}

		// 消息在磁盘
		if (isInDisk) {
			if ((bufferTotal + sizePy) > Constant.maxTransferBytesOnMessageInDisk) {
				return true;
			}

			if ((messageTotal + 1) > Constant.maxTransferBytesOnMessageInDisk) {
				return true;
			}
		}
		// 消息在内存
		else {
			if ((bufferTotal + sizePy) > Constant.maxTransferBytesOnMessageInMemory) {
				return true;
			}

			if ((messageTotal + 1) > Constant.maxTransferCountOnMessageInMemory) {
				return true;
			}
		}

		return false;
	}

	public ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>> getConsumeQueueTable() {
		return consumeQueueTable;
	}

	/**
	 * 
	 * 
	 * @param topic
	 * @param queueId
	 * @param consumeQueue
	 */
	private void putConsumeQueue(final String topic, final String queueId, final ConsumeQueue consumeQueue) {
		ConcurrentHashMap<String/* queueId */, ConsumeQueue> map = consumeQueueTable.get(topic);
		if (null == map) {
			map = new ConcurrentHashMap<String/* queueId */, ConsumeQueue>();
			map.put(queueId, consumeQueue);
			consumeQueueTable.put(topic, map);
		} else {
			map.put(queueId, consumeQueue);
		}
	}

	/**
	 * 向逻辑队列写消息
	 * 
	 * @param topic
	 * @param queueId
	 * @param offset
	 * @param size
	 * @param tagsCode
	 * @param storeTimestamp
	 * @param logicOffset
	 */
	public void putMessagePostionInfo(String topic, String queueId, long offset, int size, long storeTimestamp, long logicOffset) {
		ConsumeQueue cq = findConsumeQueue(topic, queueId);
		cq.putMessagePostionInfoWrapper(offset, size, storeTimestamp, logicOffset);
	}

	/**
	 * 恢复逻辑队列
	 */
	private void recoverConsumeQueue() {
		for (ConcurrentHashMap<String, ConsumeQueue> maps : consumeQueueTable.values()) {
			for (ConsumeQueue logic : maps.values()) {
				logic.recover();
			}
		}
	}
}
