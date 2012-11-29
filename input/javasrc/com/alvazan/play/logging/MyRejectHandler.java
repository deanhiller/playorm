package com.alvazan.play.logging;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class MyRejectHandler implements RejectedExecutionHandler {

	private AsyncAppender appender;

	public MyRejectHandler(AsyncAppender appender) {
		this.appender = appender;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		appender.addWarn("WARNINNG, the queue is full, the system will slow down now as logging is a bottleneck");
		BlockingQueue<Runnable> queue = executor.getQueue();
		try {
			queue.put(r);
		} catch (InterruptedException e) {
			appender.addError("Could not put in queue", e);
		}
	}

}
