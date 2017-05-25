package io.openmessaging.fly.store;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommitLogRun implements Runnable {

	// 是否为守护线程
	protected volatile boolean daemon = false;

	protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
	
	private MappedFileQueue queue;
	
	// 线程是否已经停止
	protected volatile boolean stoped = false;

	protected final CountDownLatch waitPoint = new CountDownLatch(1);

	public CommitLogRun(MappedFileQueue queue) {
		this.queue = queue;
	}

	public boolean isStoped() {
		return stoped;
	}

	@Override
	public void run() {
		while (!isStoped()) {
			MappedFile mf = queue.getLastMapedFileWithLock();
			mf.commit(4);
		}
	}

	public void shutdown() {
		
	}
	
	
	public void wakeup() {
		if (hasNotified.compareAndSet(false, true)) {
			waitPoint.countDown(); // notify
		}
	}
}
