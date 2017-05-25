package io.openmessaging.fly.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

public class ConsumeQueue {

	private final static Logger LOG = Logger.getLogger(ConsumeQueue.class.getName());
	
	/** 写索引时用到的ByteBuffer */
	private final ByteBuffer byteBufferIndex;

	/** 文件大小 */
	private final int mapedFileSize;

	/** 存储消息索引的队列 */
	private final MappedFileQueue mappedFileQueue;

	/** 最后一个消息对应的物理Offset */
	private long maxPhysicOffset = -1;

	/** 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset */
	/** 实际使用需要除以 StoreUnitSize */
	private volatile long minLogicOffset = 0;

	/** queueId */
	private final String queueId;

	/** 配置 */
	private final String storePath;

	/** Topic */
	private final String topic;

	/**
	 * @param topic
	 * @param queueId
	 * @param storePath
	 * @param mapedFileSize
	 * @param messageStore
	 */
	public ConsumeQueue(String topic, String queueId, String storePath, int mapedFileSize) {
		this.storePath = storePath;
		this.mapedFileSize = mapedFileSize;
		this.topic = topic;
		this.queueId = queueId;
		String queueDir = this.storePath + File.separator + topic + File.separator + queueId;
		this.mappedFileQueue = new MappedFileQueue(queueDir, mapedFileSize);
		this.byteBufferIndex = ByteBuffer.allocate(Constant.CQStoreUnitSize);
	}

	/**
	 * 执行自检工作
	 */
	public void checkSelf() {
		mappedFileQueue.checkSelf();
	}

	/**
	 * 刷新磁盘
	 * 
	 * @param flushLeastPages
	 * @return
	 */
	public boolean commit(final int flushLeastPages) {
		return this.mappedFileQueue.commit(flushLeastPages);
	}

	/**
	 * 执行销毁工作
	 */
	public void destroy() {
		this.maxPhysicOffset = -1;
		this.minLogicOffset = 0;
		this.mappedFileQueue.destroy();
	}

	/**
	 * 目标位置之前填写空数据
	 * 
	 * @param mappedFile
	 * @param untilWhere
	 */
	private void fillPreBlank(MappedFile mappedFile, long untilWhere) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Constant.CQStoreUnitSize);
		byteBuffer.putLong(0L);
		byteBuffer.putInt(Integer.MAX_VALUE);
		//byteBuffer.putLong(0L);

		int until = (int) (untilWhere % this.mappedFileQueue.getMapedFileSize());
		for (int i = 0; i < until; i += Constant.CQStoreUnitSize) {
			mappedFile.appendMessage(byteBuffer.array());
		}
	}

	/**
	 * 返回Index Buffer
	 * 
	 * @param startIndex
	 *            起始偏移量索引
	 */
	public SelectMapedBufferResult getIndexBuffer(final long startIndex) {
		int mapedFileSize = this.mapedFileSize;
		long offset = startIndex * Constant.CQStoreUnitSize;
		if (offset >= this.getMinLogicOffset()) {
			MappedFile mappedFile = this.mappedFileQueue.findMapedFileByOffset(offset);
			if (mappedFile != null) {
				SelectMapedBufferResult result = mappedFile.selectMapedBuffer((int) (offset % mapedFileSize));
				return result;
			}
		}
		return null;
	}

	/**
	 * 获取最大逻辑offset
	 * 
	 * @return
	 */
	public long getMaxOffsetInQuque() {
		return this.mappedFileQueue.getMaxOffset() / Constant.CQStoreUnitSize;
	}

	/**
	 * 获取当前队列中的消息总数
	 */
	public long getMessageTotalInQueue() {
		return this.getMaxOffsetInQuque() - this.getMinOffsetInQuque();
	}

	/**
	 * Getter method for property <tt>minLogicOffset</tt>.
	 * 
	 * @return property value of minLogicOffset
	 */
	public long getMinLogicOffset() {
		return minLogicOffset;
	}

	/**
	 * 获取最小队列offset
	 * 
	 * @return
	 */
	public long getMinOffsetInQuque() {
		return this.minLogicOffset / Constant.CQStoreUnitSize;
	}

	/**
	 * 加载mmap对象
	 * 
	 * @return
	 */
	public boolean load() {
		boolean result = this.mappedFileQueue.load();
		LOG.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
		return result;
	}

	/**
	 * 存储一个20字节的信息，putMessagePostionInfo只有一个线程调用，所以不需要加锁
	 * 
	 * @param offset
	 *            消息对应的CommitLog offset
	 * @param size
	 *            消息在CommitLog存储的大小
	 * @return 是否成功
	 */
	private boolean putMessagePostionInfo(long offset, int size, long cqOffset) {
		// 在数据恢复时会走到这个流程
		if (offset <= this.maxPhysicOffset) {
			return true;
		}
		this.byteBufferIndex.flip();
		this.byteBufferIndex.limit(Constant.CQStoreUnitSize);
		this.byteBufferIndex.putLong(offset);
		this.byteBufferIndex.putInt(size);
		long expectLogicOffset = cqOffset * Constant.CQStoreUnitSize;
		MappedFile mappedFile = this.mappedFileQueue.getLastMapedFile(expectLogicOffset);
		if (mappedFile != null) {
			// 纠正MapedFile逻辑队列索引顺序
			if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
				this.minLogicOffset = expectLogicOffset;
				this.fillPreBlank(mappedFile, expectLogicOffset);
				LOG.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " " + mappedFile.getWrotePosition());
			}

			if (cqOffset != 0) {
				long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();
				if (expectLogicOffset != currentLogicOffset) {
					LOG.warning("[BUG]logic queue order maybe wrong, expectLogicOffset:" + expectLogicOffset
							+ "currentLogicOffset:" + currentLogicOffset + "Topic:" + this.topic + "QID:" + this.queueId
							+ "Diff:" + (expectLogicOffset - currentLogicOffset));
				}
			}

			// 记录物理队列最大offset
			this.maxPhysicOffset = offset;
			return mappedFile.appendMessage(this.byteBufferIndex.array());
		}

		return false;
	}

	/**
	 * 写逻辑队列数据
	 * 
	 * @param offset
	 * @param size
	 * @param tagsCode
	 * @param storeTimestamp
	 * @param logicOffset
	 */
	public void putMessagePostionInfoWrapper(long offset, int size, long storeTimestamp, long logicOffset) {
		int MaxRetries = 30;
		for (int i = 0; i < MaxRetries; i++) {
			boolean result = putMessagePostionInfo(offset, size, logicOffset);
			if (result) {
				// messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimestamp);
				return;
			} else {
				// 只有一种情况会失败，创建新的MapedFile时报错或者超时
				LOG.warning("[BUG]put commit log postion info to " + topic + ":" + queueId + " " + offset
						+ " failed, retry " + i + " times");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.warning("" + e);
				}
			}
		}
	}

	/**
	 * 启动时恢复
	 */
	public void recover() {
		final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
		if (!mappedFiles.isEmpty()) {
			// 从倒数第三个文件开始恢复
			int index = mappedFiles.size() - 3;
			if (index < 0)
				index = 0;

			int mapedFileSizeLogics = this.mapedFileSize;
			MappedFile mappedFile = mappedFiles.get(index);
			ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
			long processOffset = mappedFile.getFileFromOffset();
			long mapedFileOffset = 0;
			while (true) {
				for (int i = 0; i < mapedFileSizeLogics; i += Constant.CQStoreUnitSize) {
					long offset = byteBuffer.getLong();
					int size = byteBuffer.getInt();

					// 说明当前存储单元有效
					// TODO 这样判断有效是否合理？
					if (offset >= 0 && size > 0) {
						mapedFileOffset = i + Constant.CQStoreUnitSize;
						this.maxPhysicOffset = offset;
					} else {
						LOG.info("recover current consume queue file over,  " + mappedFile.getFileName() + " " + offset + " " + size + " ");
						break;
					}
				}

				// 走到文件末尾，切换至下一个文件
				if (mapedFileOffset == mapedFileSizeLogics) {
					index++;
					if (index >= mappedFiles.size()) {
						// 到达最后一个文件
						LOG.info("recover last consume queue file over, last maped file " + mappedFile.getFileName());
						break;
					} else {
						mappedFile = mappedFiles.get(index);
						byteBuffer = mappedFile.sliceByteBuffer();
						processOffset = mappedFile.getFileFromOffset();
						mapedFileOffset = 0;
						LOG.info("recover next consume queue file, " + mappedFile.getFileName());
					}
				} else {
					LOG.info("recover current consume queue queue over " + mappedFile.getFileName() + " " + (processOffset + mapedFileOffset));
					break;
				}
			}

			processOffset += mapedFileOffset;
			this.mappedFileQueue.truncateDirtyFiles(processOffset);
		}
	}
}
