/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2014 All Rights Reserved.
 */
package io.openmessaging.fly.store;

/**
 * 写入消息过程的返回结果
 * 
 * @author rain.gy
 * @version $Id: PutMessageStatus.java, v 0.1 2014-11-20 下午07:33:29 rain.gy Exp $
 */
public enum PutMessageStatus {

    /** 文件相关结果 */
    CREATE_MAPEDFILE_FAILED, 
    DLQ_MSG, 
    END_OF_FILE, 
    FLUSH_DISK_TIMEOUT, 
    MESSAGE_ILLEGAL, 
    MESSAGE_SIZE_EXCEEDED, 
    PROPERTIES_SIZE_EXCEEDED, 
    PUT_OK, 
    QUEUE_NOT_SUPPORT, 
    QUEUE_SHUTDOWN_ERROR,
    QUEUE_TIMEOUT,
    SAVE_DB_ERROR,
    SERVICE_NOT_AVAILABLE,
    TEST_ERROR,
    UNKNOWN_ERROR,
    ;
}
