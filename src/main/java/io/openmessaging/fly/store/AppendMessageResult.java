package io.openmessaging.fly.store;

/**
 * 写入消息返回结果
 * 
 * @author rain.gy
 * @version $Id: AppendMessageResult.java, v 0.1 2014-11-20 下午07:36:07 rain.gy Exp $
 */
public class AppendMessageResult {

    /** 任务处理时间 */
    private long   hdTime;
    /** 消息ID */
    private String msgId;
    /** 队列offset */
    private long   offset;
    /** 队列ID */
    private String queueId;
    /** 存储处理时间 */
    private long   storeTime;
    /** 存储时间 */
    private long   storeTimestamp;
    /** 写文件字节数 */
    private int    wroteBytes;
    /** 写文件起始offset */
    private long   wroteOffset;

    /**
     * @param offset
     */
    public AppendMessageResult(long offset) {
        super();
        this.offset = offset;
    }

    /**
     * @param offset
     * @param queueId
     * @param msgId
     */
    public AppendMessageResult(long offset, String queueId, String msgId) {
        super();
        this.offset = offset;
        this.queueId = queueId;
        this.msgId = msgId;
    }

    /**
     * @param wroteOffset
     * @param wroteBytes
     * @param msgId
     * @param storeTimestamp
     * @param logicsOffset
     */
    public AppendMessageResult(String queueId, long wroteOffset, int wroteBytes, String msgId, long storeTimestamp, long logicsOffset) {
        this.queueId = queueId;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.offset = logicsOffset;
    }

    /**
     * Getter method for property <tt>hdTime</tt>.
     *
     * @return property value of hdTime
     */
    public long getHdTime() {
        return hdTime;
    }

    /**
     * Getter method for property <tt>msgId</tt>.
     * 
     * @return property value of msgId
     */
    public String getMsgId() {
        return msgId;
    }

    /**
     * Getter method for property <tt>offset</tt>.
     * 
     * @return property value of offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Getter method for property <tt>queueId</tt>.
     *
     * @return property value of queueId
     */
    public String getQueueId() {
        return queueId;
    }

    /**
     * Getter method for property <tt>storeTime</tt>.
     *
     * @return property value of storeTime
     */
    public long getStoreTime() {
        return storeTime;
    }

    /**
     * Getter method for property <tt>storeTimestamp</tt>.
     * 
     * @return property value of storeTimestamp
     */
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    /**
     * Getter method for property <tt>wroteBytes</tt>.
     * 
     * @return property value of wroteBytes
     */
    public int getWroteBytes() {
        return wroteBytes;
    }

    /**
     * Getter method for property <tt>wroteOffset</tt>.
     * 
     * @return property value of wroteOffset
     */
    public long getWroteOffset() {
        return wroteOffset;
    }

    /**
     * Setter method for property <tt>hdTime</tt>.
     * 
     * @param hdTime value to be assigned to property hdTime
     */
    public void setHdTime(long hdTime) {
        this.hdTime = hdTime;
    }

    /**
     * Setter method for property <tt>offset</tt>.
     * 
     * @param offset value to be assigned to property offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Setter method for property <tt>storeTime</tt>.
     * 
     * @param storeTime value to be assigned to property storeTime
     */
    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    /**
     * Setter method for property <tt>storeTimestamp</tt>.
     * 
     * @param storeTimestamp value to be assigned to property storeTimestamp
     */
    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    /**
     * Setter method for property <tt>wroteBytes</tt>.
     * 
     * @param wroteBytes value to be assigned to property wroteBytes
     */
    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    /**
     * Setter method for property <tt>wroteOffset</tt>.
     * 
     * @param wroteOffset value to be assigned to property wroteOffset
     */
    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "AppendMessageResult [offset=" + offset + ", queueId=" + queueId + ", msgId=" + msgId + ", wroteOffset="
               + wroteOffset + ", wroteBytes=" + wroteBytes + ", storeTimestamp=" + storeTimestamp + ", storeTime="
               + storeTime + ", hdTime=" + hdTime + "]";
    }

}
