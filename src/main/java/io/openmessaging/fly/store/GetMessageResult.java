/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2014 All Rights Reserved.
 */
package io.openmessaging.fly.store;

import java.util.ArrayList;
import java.util.List;

/**
 * 查询消息结果
 * 
 * @author guoyu
 * @version $Id: GetMessageResult.java, v 0.1 2014年11月27日 下午4:01:47 guoyu Exp $
 */
public class GetMessageResult {
	/** 序列化方式 */
	private int serializingType;
	/** 消息列表 */
	private List messageList = new ArrayList(32);
	/** 拉取消息数量 */
	private int count = 0;
	/** 拉取消息的总大小 */
	private int totalSize = 0;
	/** 枚举变量，取消息结果 */
	private String status;
	/** 逻辑队列中的最小Offset 目前文件方式有效 */
	private long minOffset = -1;
	/** 逻辑队列中的最大Offset 目前文件方式有效 */
	private long maxOffset = -1;
	/** 当被过滤后，返回下一次开始的Offset */
	private long nextBeginOffset;

	
	public GetMessageResult() {
		
	}
	
	/**
	 * 默认构造方法
	 */
	public GetMessageResult(int expected, int supported) {
		// 如果broker支持的序列化方式>=客户端请求中的序列化方式，则以客户端的请求为准
		// 如果broker支持的序列化方式<客户端请求中的序列化方式，则以broker支持的为准
		// support ==
		// Constants.SERIALIZING_TYPE_BYTE，表示broker同时支持bytebuffer和json两种方式
		// support == Constants.SERIALIZING_TYPE_JSON，表示broker只支持json两种方式
		this.serializingType = expected <= supported ? expected : supported;

	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "GetMessageResult [status=" + status + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset
				+ ", nextBeginOffset=" + nextBeginOffset + "]";
	}

	/**
	 * 在查询结果中添加一条消息.
	 *
	 * @param message
	 */
	public void addMessage(Object message) {
		this.messageList.add(message);
		this.count++;
		this.totalSize++;
	}

	public List getMessageList() {
		return messageList;
	}

	public void setMessageList(List messageList) {
		this.messageList = messageList;
	}

	/**
	 * 在查询结果中添加一批消息.
	 *
	 * @param messages
	 */
	public void addMessages(List<Object> messages) {
		if (null != messages && !messages.isEmpty()) {
			this.messageList.addAll(messages);
			this.count += messages.size();
			for (Object message : messages) {
				if (message != null) {
					this.totalSize++;
				}
			}
		}
	}

	/**
	 * Getter method for property <tt>nextBeginOffset</tt>.
	 * 
	 * @return property value of nextBeginOffset
	 */
	public long getNextBeginOffset() {
		return nextBeginOffset;
	}

	/**
	 * Setter method for property <tt>nextBeginOffset</tt>.
	 * 
	 * @param nextBeginOffset
	 *            value to be assigned to property nextBeginOffset
	 */
	public void setNextBeginOffset(long nextBeginOffset) {
		this.nextBeginOffset = nextBeginOffset;
	}

	/**
	 * Getter method for property <tt>minOffset</tt>.
	 * 
	 * @return property value of minOffset
	 */
	public long getMinOffset() {
		return minOffset;
	}

	/**
	 * Setter method for property <tt>minOffset</tt>.
	 * 
	 * @param minOffset
	 *            value to be assigned to property minOffset
	 */
	public void setMinOffset(long minOffset) {
		this.minOffset = minOffset;
	}

	/**
	 * Getter method for property <tt>maxOffset</tt>.
	 * 
	 * @return property value of maxOffset
	 */
	public long getMaxOffset() {
		return maxOffset;
	}

	/**
	 * Setter method for property <tt>maxOffset</tt>.
	 * 
	 * @param maxOffset
	 *            value to be assigned to property maxOffset
	 */
	public void setMaxOffset(long maxOffset) {
		this.maxOffset = maxOffset;
	}

	public int getTotalSize() {
		return totalSize;
	}

	public int getCount() {
		return count;
	}
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
