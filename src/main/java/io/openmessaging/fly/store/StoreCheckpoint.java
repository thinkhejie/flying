package io.openmessaging.fly.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 记录存储模型最终一致的时间点
 * 
 * @author guoyu
 * @version $Id: StoreCheckpoint.java, v 0.1 2015年12月7日 下午8:12:16 guoyu Exp $
 */
public class StoreCheckpoint {
	
	private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(StoreCheckpoint.class.getName());
			
    /** 文件通道 */
    private final FileChannel       fileChannel;
    
    /** 逻辑消息一致时间点 */
    private volatile long           logicsMsgTimestamp = 0;
    
    /** 内存映射文件对象 */
    private final MappedByteBuffer  mappedByteBuffer;

    /** 物理消息一致时间点 */
    private volatile long           physicMsgTimestamp = 0;
    
    /** 文件操作对象 */
    private final RandomAccessFile  randomAccessFile;

    /**
     * @param scpPath 文件地址
     * @throws IOException
     */
    public StoreCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, Constant.OS_PAGE_SIZE);

        if (fileExists) {
            LOG.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);

            LOG.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                     + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            LOG.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                     + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
        } else {
            LOG.info("store checkpoint file not exists, " + scpPath);
        }
    }

    /**
     * 刷新磁盘
     */
    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.force();
    }

    /**
     * Getter method for property <tt>logicsMsgTimestamp</tt>.
     * 
     * @return property value of logicsMsgTimestamp
     */
    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }

    /**
     * 获取最小一致时间
     * 
     * @return
     */
    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        // 向前倒退3s，防止因为时间精度问题导致丢数据
        // fixed https://github.com/alibaba/RocketMQ/issues/467
        min -= 1000 * 3;
        if (min < 0)
            min = 0;

        return min;
    }

    /**
     * Getter method for property <tt>physicMsgTimestamp</tt>.
     * 
     * @return property value of physicMsgTimestamp
     */
    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }

    /**
     * Setter method for property <tt>logicsMsgTimestamp</tt>.
     * 
     * @param logicsMsgTimestamp value to be assigned to property logicsMsgTimestamp
     */
    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }

    /**
     * Setter method for property <tt>physicMsgTimestamp</tt>.
     * 
     * @param physicMsgTimestamp value to be assigned to property physicMsgTimestamp
     */
    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }

    /**
     * 关闭服务
     */
    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            LOG.severe("check point close error." + e);
        }
    }

}
