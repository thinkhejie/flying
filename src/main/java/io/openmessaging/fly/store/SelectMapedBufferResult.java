/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2015 All Rights Reserved.
 */
package io.openmessaging.fly.store;

import java.nio.ByteBuffer;

/**
 * 查询Pagecache返回结果
 * 
 * @author guoyu
 * @version $Id: SelectMapedBufferResult.java, v 0.1 2015年12月4日 下午3:48:01 guoyu Exp $
 */
public class SelectMapedBufferResult {

    /** position从0开始 */
    private final ByteBuffer byteBuffer;
    /** 用来释放内存 */
    private MappedFile       mappedFile;
    /** 有效数据大小 */
    private int              size;
    /** 从队列中哪个绝对Offset开始 */
    private final long       startOffset;

    /**
     * @param startOffset
     * @param byteBuffer
     * @param size
     * @param mappedFile
     */
    public SelectMapedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    /**
     * Getter method for property <tt>byteBuffer</tt>.
     *
     * @return property value of byteBuffer
     */
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    /**
     * Getter method for property <tt>mappedFile</tt>.
     * 
     * @return property value of mappedFile
     */
    public MappedFile getMappedFile() {
        return mappedFile;
    }

    /**
     * Getter method for property <tt>size</tt>.
     *
     * @return property value of size
     */
    public int getSize() {
        return size;
    }

    /**
     * Getter method for property <tt>startOffset</tt>.
     *
     * @return property value of startOffset
     */
    public long getStartOffset() {
        return startOffset;
    }

    /**
     * Setter method for property <tt>size</tt>.
     *
     * @param size value to be assigned to property size
     */
    public void setSize(int size) {
        this.size = size;
        this.byteBuffer.limit(this.size);
    }

    /**
     * 此方法只能被调用一次，重复调用无效
     */
    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }
}
