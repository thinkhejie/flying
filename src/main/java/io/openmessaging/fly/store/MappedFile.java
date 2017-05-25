package io.openmessaging.fly.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class MappedFile {
	
	/** 日志对象 */
	private static final Logger LOG = Logger.getLogger(MappedFile.class.getName());

	/** 未知错误结果 */
	public static final PutMessageResult PUT_UNKNOWN_ERROR_RESULT = new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);

	/** 当前JVM中mmap句柄数量 */
	private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);

	/** 当前JVM中映射的虚拟内存总大小 */
	private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);

	/** 是否可用 */
	protected volatile boolean available = true;

	/** 是否清理完成 */
	protected volatile boolean cleanupOver = false;

	/** Flush到什么位置 */
	private final AtomicInteger committedPosition = new AtomicInteger(0);

	/** 映射的文件 */
	private final File file;

	/** 映射的FileChannel对象 */
	private FileChannel fileChannel;

	/** 映射的起始偏移量 */
	private final long fileFromOffset;

	/** 映射的文件名 */
	private final String fileName;

	/** 映射的文件大小，定长 */
	private final int fileSize;

	/** 创建的第一个队列文件 */
	private boolean firstCreateInQueue = false;

	/** 第一次执行关闭的时间 */
	private volatile long firstShutdownTimestamp = 0;

	/** 映射的内存对象，position永远不变 */
	private final MappedByteBuffer mappedByteBuffer;

	/** 引用计数 */
	protected final AtomicLong refCount = new AtomicLong(1);

	/** 最后一条消息存储时间 */
	private volatile long storeTimestamp = 0;

	/** 当前写到什么位置 */
	private final AtomicInteger wrotePostion = new AtomicInteger(0);
	
	/**
	 * 清除内存映射文件
	 * 
	 * @param buffer
	 */
	public static void clean(final ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
			return;
		}
		UtilAll.invoke(UtilAll.invoke(UtilAll.viewed(buffer), "cleaner"), "clean");
	}

	/**
	 * 确认父目录存在，不存在则创建
	 * 
	 * @param dirName
	 */
	public static void ensureDirOK(final String dirName) {
		if (dirName != null && (!"".equals(dirName))) {
			File f = new File(dirName);
			if (!f.exists()) {
				boolean result = f.mkdirs();
				LOG.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
			}
		}
	}

	/**
	 * @param fileName
	 *            文件名称
	 * @param fileSize
	 *            文件大小
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public MappedFile(String fileName, int fileSize) throws IOException {
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.file = new File(fileName);
		this.fileFromOffset = Long.parseLong(this.file.getName());
		boolean ok = false;

		// 确认父目录存在
		ensureDirOK(this.file.getParent());

		try {
			this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
			this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
			TotalMapedVitualMemory.addAndGet(fileSize);
			TotalMapedFiles.incrementAndGet();
			ok = true;
		} catch (FileNotFoundException e) {
			LOG.severe("create file channel " + this.fileName + " Failed. " + e);
			throw e;
		} catch (IOException e) {
			LOG.severe("map file " + this.fileName + " Failed. " + e);
			throw e;
		} finally {
			if (!ok && this.fileChannel != null) {
				this.fileChannel.close();
			}
		}
	}

	/**
	 * 向MapedBuffer追加消息<br>
	 *
	 * @param msg
	 *            要追加的消息
	 * @param cb
	 *            用来对消息进行序列化，尤其对于依赖MapedFile Offset的属性进行动态序列化
	 * @return 是否成功，写入多少数据
	 */
	public PutMessageResult appendMessage(Object msg, DefaultAppendMessageCallback cb) {
		int currentPos = this.wrotePostion.get();

		// 表示有空余空间
		if (currentPos < this.fileSize) {
			ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
			byteBuffer.position(currentPos);
			PutMessageResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
			if (result.getAppendMessageResult() != null) {
				this.wrotePostion.addAndGet(result.getAppendMessageResult().getWroteBytes());
				this.storeTimestamp = result.getAppendMessageResult().getStoreTimestamp();
			}

			return result;
		}

		// 上层应用应该保证不会走到这里
		LOG.severe("MappedFile.appendMessage return null, wrotePostion: " + currentPos + " fileSize: " + this.fileSize);
		return PUT_UNKNOWN_ERROR_RESULT;
	}

	/**
	 * 执行清理任务
	 * 
	 * @param currentRef
	 * @return
	 */
	public boolean cleanup(long currentRef) {
		// 如果没有被shutdown，则不可以unmap文件，否则会crash
		if (this.isAvailable()) {
			LOG.severe("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmaping.");
			return false;
		}

		// 如果已经cleanup，再次操作会引起crash
		if (this.isCleanupOver()) {
			LOG.severe("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
			// 必须返回true
			return true;
		}

		clean(this.mappedByteBuffer);
		TotalMapedVitualMemory.addAndGet(this.fileSize * (-1));
		TotalMapedFiles.decrementAndGet();
		LOG.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
		return true;
	}

	/**
	 * 消息刷盘
	 *
	 * @param flushLeastPages
	 *            至少刷几个page
	 * @return
	 */
	public int commit(int flushLeastPages) {
		if (this.isAbleToFlush(flushLeastPages)) {
			if (this.hold()) {
				int value = this.wrotePostion.get();
				this.mappedByteBuffer.force();
				this.committedPosition.set(value);
				this.release();
			} else {
				LOG.warning("in commit, hold failed, commit offset = " + this.committedPosition.get());
				this.committedPosition.set(this.wrotePostion.get());
			}
		}

		return this.getCommittedPosition();
	}

	/**
	 * 获取FLUSH位置
	 *
	 * @return
	 */
	public int getCommittedPosition() {
		return committedPosition.get();
	}

	/**
	 * 文件起始偏移量
	 */
	public long getFileFromOffset() {
		return this.fileFromOffset;
	}

	/**
	 * 获取文件名称
	 * 
	 * @return
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * 获取引用个数
	 * 
	 * @return
	 */
	public long getRefCount() {
		return this.refCount.get();
	}

	public long getStoreTimestamp() {
		return storeTimestamp;
	}

	/**
	 * 资源是否能HOLD住
	 */
	public synchronized boolean hold() {
		if (this.isAvailable()) {
			if (this.refCount.getAndIncrement() > 0) {
				return true;
			} else {
				this.refCount.getAndDecrement();
			}
		}
		return false;
	}

	/**
	 * 是否可以刷盘
	 *
	 * @param flushLeastPages
	 * @return
	 */
	private boolean isAbleToFlush(int flushLeastPages) {
		int flush = this.committedPosition.get();
		int write = this.wrotePostion.get();

		// 如果当前文件已经写满，应该立刻刷盘
		if (this.isFull()) {
			return true;
		}

		// 只有未刷盘数据满足指定page数目才刷盘
		if (flushLeastPages > 0) {
			return ((write / Constant.OS_PAGE_SIZE) - (flush / Constant.OS_PAGE_SIZE)) >= flushLeastPages;
		}

		return write > flush;
	}

	/**
	 * 资源是否可用，即是否可被HOLD
	 */
	public boolean isAvailable() {
		return this.available;
	}

	/**
	 * 资源是否被清理完成
	 */
	public boolean isCleanupOver() {
		return this.refCount.get() <= 0 && this.cleanupOver;
	}

	/**
     * 是否第一个创建的队列文件
     * 
     * @return
     */
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

	/**
	 * 是否当前文件已经写满
	 *
	 * @return
	 */
	public boolean isFull() {
		return this.fileSize == this.wrotePostion.get();
	}

	/**
	 * 释放资源
	 */
	public void release() {
		long value = this.refCount.decrementAndGet();
		if (value > 0)
			return;

		synchronized (this) {
			// cleanup内部要对是否clean做处理
			this.cleanupOver = this.cleanup(value);
		}
	}

	/**
     * 读目标位置字节缓冲
     *
     * @param pos
     * @return
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos) {
        if (pos < this.wrotePostion.get() && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = this.wrotePostion.get() - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }

	/**
	 * 设置FLUSH位置
	 *
	 * @param pos
	 */
	public void setCommittedPosition(int pos) {
		this.committedPosition.set(pos);
	}

	public void setFirstCreateInQueue(boolean firstCreateInQueue) {
		this.firstCreateInQueue = firstCreateInQueue;
	}
	
    /**
	 * 设置当前写到什么位置
	 * 
	 * @param pos
	 */
	public void setWrotePostion(int pos) {
		this.wrotePostion.set(pos);
	}

	/**
	 * 禁止资源被访问 shutdown不允许调用多次，最好是由管理线程调用
	 */
	public void shutdown(long intervalForcibly) {
		if (this.available) {
			this.available = false;
			this.firstShutdownTimestamp = System.currentTimeMillis();
			this.release();
		}
		// 强制shutdown
		else if (this.getRefCount() > 0) {
			if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
				this.refCount.set(-1000 - this.getRefCount());
				this.release();
			}
		}
	}
	
    /**
	 * 每隔 OS_PAGE_SIZE(1024*4) 预写一次
	 *
	 * @return
	 */
	public void warmMappedFile(int pages) {
		long beginTime = System.currentTimeMillis();
		ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
		long time = System.currentTimeMillis();
		for (int i = 0, j = 0; i < this.fileSize; i += Constant.OS_PAGE_SIZE, j++) {
			byteBuffer.put(i, (byte) 0);
			// prevent gc
			if (j % 1000 == 0) {
				LOG.info("j=" + j + ", costTime=" + (System.currentTimeMillis() - time));
				time = System.currentTimeMillis();
				try {
					Thread.sleep(0);
				} catch (InterruptedException e) {
					// DO NOTHING
				}
			}
		}
		LOG.info("mapped file worm up done. mappedFile=" +  this.getFileName() + "+, costTime=" + (System.currentTimeMillis() - beginTime));
	}

	public boolean appendMessage(byte[] data) {
        int currentPos = this.wrotePostion.get();
        // 表示有空余空间
        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePostion.addAndGet(data.length);
            return true;
        }
        return false;
	}

	public long getWrotePosition() {
		return wrotePostion.get();
	}

    /**
     * 获取指定位置的字节缓冲
     *
     * @param pos
     * @param size
     * @return
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos, int size) {
        // 有消息
        if ((pos + size) <= this.wrotePostion.get()) {
            // 从MapedBuffer读
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                LOG.warning("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            LOG.warning("selectMapedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }
    
    /**
     * 方法不能在运行时调用，不安全。只在启动时，reload已有数据时调用
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }
    
    /**
     * 清理资源，destroy与调用shutdown的线程必须是同一个
     *
     * @return 是否被destory成功，上层调用需要对失败情况处理，失败后尝试重试
     */
    public boolean destroy(long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                LOG.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                LOG.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                         + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                         + this.getCommittedPosition() + ", " + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                LOG.warning("close file channel " + this.fileName + " Failed. " + e);
            }

            return true;
        } else {
            LOG.warning("destroy maped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }
}
