package io.openmessaging.fly.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class MappedFileQueue {

	private final static Logger LOG = Logger.getLogger(MappedFileQueue.class.getName());
    
    /** 刷盘刷到哪里 */
    private long                           committedWhere      = 0;

	/** ConsumeQueue集合 KEY1:TOPIC KEY2:QUEUEID */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>> consumeQueueTable;

	private final int mapedFileSize;

	private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	
	private final String storePath;
    
    /** 最后一条消息存储时间 */
    private volatile long                  storeTimestamp      = 0;

	/**
	 * @param storePath
	 * @param mapedFileSize
	 * @param allocateMapedFileService
	 */
	public MappedFileQueue(String storePath, int mapedFileSize) {
		this.storePath = storePath;
		this.mapedFileSize = mapedFileSize;
		this.consumeQueueTable = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>>(10);
	}

	/**
     * 返回值表示是否全部刷盘完成
     * 
     * @return
     */
    public boolean commit(int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMapedFileByOffset(this.committedWhere, true);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.commit(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = (where == this.committedWhere);
            this.committedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

	/**
     * 根据offset获取MapedFile对象
     * 
     * @param offset
     * @return
     */
    public MappedFile findMapedFileByOffset(long offset) {
        return findMapedFileByOffset(offset, false);
    }

	/**
     * 根据offset返回MapedFile对象
     * 
     * @param offset
     * @param returnFirstOnNotFound
     * @return
     */
    public MappedFile findMapedFileByOffset(long offset, boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MappedFile mappedFile = this.getFirstMapedFile();

            if (mappedFile != null) {
                int index = (int) ((offset / this.mapedFileSize) - (mappedFile.getFileFromOffset() / this.mapedFileSize));
                if (index < 0 || index >= this.mappedFiles.size()) {
                    LOG.warning("findMapedFileByOffset offset not matched, request Offset:" + offset + ", index:" + index + "mapedFileSize:" + this.mapedFileSize + ", mappedFiles count:" + this.mappedFiles.size() + ", StackTrace:"+ UtilAll.currentStackTrace());
                }

                try {
                    return this.mappedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mappedFile;
                    }
                }
            }
        } catch (Exception e) {
            LOG.severe("findMapedFileByOffset Exception:" + e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }
	
	/**
     * 获取逻辑队列集合
     * 
     * @return
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>> getConsumeQueueTable() {
		return consumeQueueTable;
    }
	
	/**
     * 获取第一个MapedFile对象
     * 
     * @return
     */
    private MappedFile getFirstMapedFile() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }

        return this.mappedFiles.get(0);
    }
	
	public MappedFile getLastMapedFile() {
		return getLastMapedFile(0);
	}
	
    public MappedFile getLastMapedFile(long startOffset) {
		return getLastMapedFile(startOffset, true);
	}
    
    /**
	 * 获取最后一个MapedFile对象，如果一个都没有，则新创建一个，如果最后一个写满了，则新创建一个
	 * 
	 * @param startOffset
	 *            如果创建新的文件，起始offset
	 * @return
	 */
	public MappedFile getLastMapedFile(long startOffset, boolean needCreate) {
		long createOffset = -1;
		MappedFile mappedFileLast = null;
		{
			this.readWriteLock.readLock().lock();
			if (this.mappedFiles.isEmpty()) {
				createOffset = startOffset - (startOffset % this.mapedFileSize);
			} else {
				mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
			}
			this.readWriteLock.readLock().unlock();
		}

		if (mappedFileLast != null && mappedFileLast.isFull()) {
			createOffset = mappedFileLast.getFileFromOffset() + this.mapedFileSize;
		}

		if (createOffset != -1 && needCreate) {
			String nextFilePath = getPath(this.storePath) + File.separator + UtilAll.offset2FileName(createOffset);
			//String nextNextFilePath = getPath(this.storePath) + File.separator + UtilAll.offset2FileName(createOffset + this.mapedFileSize);
			MappedFile mappedFile = null;
			try {
				mappedFile = new MappedFile(nextFilePath, this.mapedFileSize);
			} catch (IOException e) {
				LOG.severe("create mapedfile exception" + e);
			}

			if (mappedFile != null) {
				this.readWriteLock.writeLock().lock();
				if (this.mappedFiles.isEmpty()) {
					mappedFile.setFirstCreateInQueue(true);
				}
				this.mappedFiles.add(mappedFile);
				this.readWriteLock.writeLock().unlock();
			}

			return mappedFile;
		}

		return mappedFileLast;
	}
    
    /**
	 * 
	 * @return
	 */
	public MappedFile getLastMapedFileWithLock() {
		MappedFile mappedFileLast = null;
		this.readWriteLock.readLock().lock();
		if (!this.mappedFiles.isEmpty()) {
			mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
		}
		this.readWriteLock.readLock().unlock();

		return mappedFileLast;
	}
    
    /**
     * Getter method for property <tt>storeTimestamp</tt>.
     * 
     * @return property value of storeTimestamp
     */
    public long getStoreTimestamp() {
        return storeTimestamp;
    }
	
	String getPath(String rootDir) {
		return rootDir;
	}
   
    public boolean load() {
		File dir = new File(getPath(this.storePath));
		File[] files = dir.listFiles();
		if (files != null) {
			// ascending order
			Arrays.sort(files);
			for (File file : files) {
				// 校验文件大小是否匹配
				if (file.length() != this.mapedFileSize) {
					LOG.warning(file + "\t" + file.length() + " length not matched message store config value, ignore it");
					return true;
				}

				// 恢复队列
				try {
					MappedFile mappedFile = new MappedFile(file.getPath(), mapedFileSize);
					mappedFile.setWrotePostion(this.mapedFileSize);
					mappedFile.setCommittedPosition(this.mapedFileSize);
					this.mappedFiles.add(mappedFile);
					LOG.info("load " + file.getPath() + " OK");
				} catch (IOException e) {
					LOG.severe("load file " + file + " error:" + e);
					return false;
				}
			}
		}

		return true;
	}
    
    /**
	 * 关闭队列，队列数据还在，但是不能访问
	 */
	public void shutdown(long intervalForcibly) {
		this.readWriteLock.readLock().lock();
		for (MappedFile mf : this.mappedFiles) {
			mf.shutdown(intervalForcibly);
		}
		this.readWriteLock.readLock().unlock();
	}

	public void checkSelf() {
		
	}

	public void destroy() {
		
	}

	public long getMapedFileSize() {
		return 0;
	}
	
    /**
     * 获取队列的最大offset
     * 
     * @return
     */
    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mappedFiles.isEmpty()) {
                int lastIndex = this.mappedFiles.size() - 1;
                MappedFile mappedFile = this.mappedFiles.get(lastIndex);
                return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
            }
        } catch (Exception e) {
            LOG.severe("getMinOffset has exception." + e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }
    
    /**
     * 获取下一个文件的起始offset
     * 
     * @param offset
     * @return
     */
    public long rollNextFile(long offset) {
        int tempMapedFileSize = mapedFileSize;
        return (offset + tempMapedFileSize - offset % tempMapedFileSize);
    }
    
    /**
     * 根据offset和size查询消息
     * 
     * @param offset
     * @param size
     * @return
     */
    public SelectMapedBufferResult getMessage(long offset, int size) {
        int mapedFileSize = this.mapedFileSize;
        MappedFile mappedFile = findMapedFileByOffset(offset, (0 == offset ? true : false));
        if (mappedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mappedFile.selectMapedBuffer(pos, size);
            return result;
        }
        return null;
    }
    
    /**
     * Getter method for property <tt>mappedFiles</tt>.
     * 
     * @return property value of mappedFiles
     */
    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }
    
    /**
     * recover时调用，不需要加锁
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mapedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePostion((int) (offset % this.mapedFileSize));
                    file.setCommittedPosition((int) (offset % this.mapedFileSize));
                } else {
                    // 将文件删除掉
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }
    
    
    /**
     * 删除文件只能从头开始删
     */
    private void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MappedFile file : files) {
                    if (!this.mappedFiles.remove(file)) {
                        LOG.severe("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.severe("deleteExpiredFile has exception." + e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }
}
