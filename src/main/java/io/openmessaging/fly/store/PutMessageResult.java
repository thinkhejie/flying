/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2014 All Rights Reserved.
 */
package io.openmessaging.fly.store;

/**
 * 写入消息返回结果
 * 
 * @author rain.gy
 * @version $Id: PutMessageResult.java, v 0.1 2014-11-20 下午07:35:13 rain.gy Exp $
 */
public class PutMessageResult {

    /** 存储DB异常结果 */
    public static final PutMessageResult PUT_ERROR_RESULT = new PutMessageResult(
                                                              PutMessageStatus.SAVE_DB_ERROR, null);
    /** 向DB写入消息返回结果 */
    private AppendMessageResult          appendMessageResult;
    /** 写入消息过程的返回结果 */
    private PutMessageStatus             putMessageStatus;

    /** 描述 */
    private String                       remark;

    /**
     * @param putMessageStatus
     * @param appendMessageResult
     */
    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        super();
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    /**
     * @param putMessageStatus
     * @param appendMessageResult
     * @param remark
     */
    public PutMessageResult(PutMessageStatus putMessageStatus, String remark,
                            AppendMessageResult appendMessageResult) {
        super();
        this.putMessageStatus = putMessageStatus;
        this.remark = remark;
        this.appendMessageResult = appendMessageResult;
    }

    /**
     * Getter method for property <tt>appendMessageResult</tt>.
     * 
     * @return property value of appendMessageResult
     */
    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }

    /**
     * Getter method for property <tt>putMessageStatus</tt>.
     * 
     * @return property value of putMessageStatus
     */
    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }

    /**
     * Getter method for property <tt>remark</tt>.
     * 
     * @return property value of remark
     */
    public String getRemark() {
        return remark;
    }

    /**
     * Setter method for property <tt>putMessageStatus</tt>.
     * 
     * @param putMessageStatus value to be assigned to property putMessageStatus
     */
    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }

    /** 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PutMessageResult [putMessageStatus=" + putMessageStatus + ", appendMessageResult="
               + appendMessageResult + ", remark=" + remark + "]";
    }

}
