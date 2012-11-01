package com.alvazan.play.logging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

public class AsyncAppender extends AppenderBase<ILoggingEvent> {

	private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();
	private int appenderCount;

	private int backupLength = 5000;
	private ExecutorService exec;

	public void addAppender(Appender<ILoggingEvent> newAppender) {
		if (appenderCount == 0) {
			appenderCount++;
			addInfo("Attaching appender named [" + newAppender.getName()
					+ "] to appender ["+getName()+"].");
			aai.addAppender(newAppender);
		} else {
			addWarn("One and only one appender may be attached to AsyncAppender name=["+getName()+"].");
			addWarn("Ignoring additional appender named ["
					+ newAppender.getName() + "]");
		}
	}

	public void setBackupLength(int queueSize) {
		this.backupLength = queueSize;
	}

	@Override
	public void start() {
		if (appenderCount == 0) {
			addError("No attached appenders found.");
			return;
		}
		
		if (backupLength < 1) {
			addError("Invalid backupLength size [" + backupLength + "]");
			return;
		}

		RejectedExecutionHandler handler = new MyRejectHandler(this);
		LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(backupLength);
		exec = new ThreadPoolExecutor(1,1,0, TimeUnit.MILLISECONDS, queue, handler);

		super.start();
	}
	
	@Override
	public void stop() {
		super.stop();
		exec.shutdownNow();
		aai.detachAndStopAllAppenders();
	}

	@Override
	protected void append(ILoggingEvent evt) {
		Runnable r = new AsyncRun(evt);
		exec.execute(r);
	}

	private class AsyncRun implements Runnable {
		private ILoggingEvent evt;

		public AsyncRun(ILoggingEvent evt) {
			this.evt = evt;
		}

		@Override
		public void run() {
			try {
				aai.appendLoopOnAppenders(evt);
			} catch(Exception e) {
				addError("Exception trying to log", e);
			}
		}
	}
}
