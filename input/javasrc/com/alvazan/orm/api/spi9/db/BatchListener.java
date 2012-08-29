package com.alvazan.orm.api.spi9.db;

public interface BatchListener {

	public void beforeFetchingNextBatch();
	
	public void afterFetchingNextBatch(int numFetched);
	
}
