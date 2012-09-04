package com.alvazan.orm.api.z5api;

import com.alvazan.orm.api.z8spi.AbstractCursor;



public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	public AbstractCursor<IndexColumnInfo> getResultList();

	/**
	 * The Iterable from getResults() is only loaded with 'batchSize' at a time from the nosql store so as you iterate
	 * GC should be releasing memory for the previous 500 while the Iterable loads the next 500.
	 * 
	 * @param batchSize
	 */
	public void setBatchSize(int batchSize);
}
