package io.openmessaging.fly.store;

public class Constant {

	public static final int CQStoreUnitSize = 12;
	
	/** 系统PAGE CACGE字节数 */
	public static final int OS_PAGE_SIZE = 1024 * 4;
	
	/** ConsumeQueue每个文件大小 默认存储40000000W条消息 */
	public static final int mapedFileSizeConsumeQueue = 40000000 * 12;

	/** 命中消息在内存的最大比例 */
	public static final int accessMessageInMemoryMaxRatio = 40;

	/** 最大被拉取的消息字节数，消息在磁盘 */
	public static final int maxTransferBytesOnMessageInDisk = 1024 * 64;

	/** 最大被拉取的消息个数，消息在磁盘 */
	public static final int maxTransferCountOnMessageInDisk = 8;

	/** 最大被拉取的消息字节数，消息在内存 */
	public static final int maxTransferBytesOnMessageInMemory = 1024 * 256;

	/** 最大被拉取的消息个数，消息在内存 */
	public static final int maxTransferCountOnMessageInMemory = 32;
}
