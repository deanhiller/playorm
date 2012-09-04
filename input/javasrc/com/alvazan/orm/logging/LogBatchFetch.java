package com.alvazan.orm.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;

public class LogBatchFetch implements BatchListener {

	private static final Logger log = LoggerFactory.getLogger(LogBatchFetch.class);
	
	private BatchListener listener;
	private long startTime;
	private Integer batchSize;

	public LogBatchFetch(BatchListener l, Integer batchSize) {
		this.listener = l;
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFetchingNextBatch() {
		if(listener != null)
			listener.beforeFetchingNextBatch();
		startTime = System.currentTimeMillis();
	}

	@Override
	public void afterFetchingNextBatch(int numFetched) {
		if(log.isInfoEnabled()) {
			String bSize = "all";
			if(batchSize != null)
				bSize = batchSize+"";
			long total = System.currentTimeMillis() - startTime;
			log.info("Fetching batch took="+total+" ms for batchSize="+bSize+" numFetched="+numFetched);
		}
		if(listener != null)
			listener.afterFetchingNextBatch(numFetched);
	}

}
