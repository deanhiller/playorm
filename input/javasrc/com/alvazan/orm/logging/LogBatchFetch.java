package com.alvazan.orm.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;

public class LogBatchFetch implements BatchListener {

	private static final Logger log = LoggerFactory.getLogger(LogBatchFetch.class);
	
	private BatchListener listener;
	private long startTime;
	private Integer batchSize;
	private String cfAndIndex;
	private ScanType findType;

	public LogBatchFetch(String cfAndIndex, BatchListener l, Integer batchSize, ScanType findType) {
		this.findType = findType;
		this.cfAndIndex = cfAndIndex;
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
			logInfo(numFetched, bSize, total);
		}
		if(listener != null)
			listener.afterFetchingNextBatch(numFetched);
	}

	private void logInfo(int numFetched, String bSize, long total) {
		switch (findType) {
		case FIND:
			log.info("[rawlogger]"+cfAndIndex+" Find took="+total+" ms for batchSize="+bSize+" numFetched="+numFetched);
			break;
		case COLUMN_SLICE:
			log.info("[rawlogger]"+cfAndIndex+" Column slice took="+total+" ms for batchSize="+bSize+" numFetched="+numFetched);
			break;
		case RANGE_SLICE:
			log.info("[rawlogger]"+cfAndIndex+" Index slice took="+total+" ms for batchSize="+bSize+" numFetched="+numFetched);
			break;
		case NON_CONTIGUOUS:
			log.info("[rawlogger]"+cfAndIndex+" Non-contiguous columns fetch took="+total+" ms for batchSize="+bSize+" numFetched="+numFetched);
			break;
		default:
			break;
		}
	}
}
