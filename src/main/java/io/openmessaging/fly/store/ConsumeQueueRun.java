package io.openmessaging.fly.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import io.openmessaging.KeyValue;

/**
 * 逻辑队列刷盘服务
 * 
 * @author guoyu
 * @version $Id: FlushConsumeQueueService.java, v 0.1 2015年12月9日 下午4:43:19 guoyu Exp $
 */
public class ConsumeQueueRun implements Runnable {
    /** 日志 */
    private static final java.util.logging.Logger LOG = Logger.getLogger(ConsumeQueueRun.class.getName());
    
    /** 重试N次后全部刷盘 */
    private static final int        RetryTimesOver     = 3;

    private int flushConsumeQueueLeastPages;
    
    private int flushConsumeQueueThoroughInterval;
    
	private int flushIntervalConsumeQueue;
	
	/** 上一次刷盘时间 */
    private long                    lastFlushTimestamp = 0;
	
	/** 存储服务层 */
    private final FlyingStore  messageStoreServer;
	
	private String rootDir;
	
	// 线程是否已经停止
	protected volatile boolean stoped = false;
	
	private StoreCheckpoint storeCheckpoint;

    public ConsumeQueueRun(FlyingStore flyingStore, KeyValue keyValue) throws IOException {
        this.messageStoreServer = flyingStore;
        this.flushIntervalConsumeQueue = keyValue.getInt("FLUSH_INTERVAL_CONSUME_QUEUE");
        this.flushConsumeQueueLeastPages = keyValue.getInt("FLUSH_CONSUME_QUEUE_LEASTPAGES");
        this.flushConsumeQueueThoroughInterval = keyValue.getInt("FLUSH_CONSUME_QUEUE_THOROUGHINTERVAL");
        this.rootDir = keyValue.getString("STORE_PATH");
        this.storeCheckpoint = new StoreCheckpoint(rootDir + File.separator + "checkpoint");
    }

    /**
     * 执行刷盘
     * 
     * @param retryTimes
     */
    private void doFlush(int retryTimes) {
        /**
         * 变量含义：如果大于0，则标识这次刷盘必须刷多少个page，如果=0，则有多少刷多少
         */
        int flushConsumeQueueLeastPages = this.flushConsumeQueueLeastPages;

        if (retryTimes == RetryTimesOver) {
            flushConsumeQueueLeastPages = 0;
        }

        long logicsMsgTimestamp = 0;

        // 定时刷盘
        int flushConsumeQueueThoroughInterval = this.flushConsumeQueueThoroughInterval;
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
            this.lastFlushTimestamp = currentTimeMillis;
            flushConsumeQueueLeastPages = 0;
            logicsMsgTimestamp = getStoreCheckpoint().getLogicsMsgTimestamp();
        }

        ConcurrentHashMap<String, ConcurrentHashMap<String, ConsumeQueue>> tables = messageStoreServer.getConsumeQueueTable();

        for (ConcurrentHashMap<String, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue cq : maps.values()) {
                boolean result = false;
                for (int i = 0; i < retryTimes && !result; i++) {
                    result = cq.commit(flushConsumeQueueLeastPages);
                }
            }
        }

        if (0 == flushConsumeQueueLeastPages) {
            if (logicsMsgTimestamp > 0) {
                getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
            }
            getStoreCheckpoint().flush();
        }
    }

    public long getJointime() {
        return 1000 * 60;
    }

	private String getServiceName() {
		return null;
	}

	/**
     * 获取刷盘时间存储路径
     * 
     * @param rootDir
     * @return
     */
    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public boolean isStoped() {
		return stoped;
	}
    
	/**
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        LOG.info("service started");

        while (!this.isStoped()) {
            try {
                int interval = flushIntervalConsumeQueue;
                this.waitForRunning(interval);
                this.doFlush(1);
            } catch (Exception e) {
                LOG.severe("service has exception. " + e.getMessage());
            }
        }

        // 正常shutdown时，要保证全部刷盘才退出
        this.doFlush(RetryTimesOver);

        LOG.info(this.getServiceName() + " service end");
    }
	
    public void setStoreCheckpoint(StoreCheckpoint storeCheckpoint) {
		this.storeCheckpoint = storeCheckpoint;
	}
    
	private void waitForRunning(int interval) {
		
	}
}
