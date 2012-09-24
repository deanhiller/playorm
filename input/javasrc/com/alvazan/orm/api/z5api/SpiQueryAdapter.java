package com.alvazan.orm.api.z5api;

import java.util.Set;

import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;


public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	/**
	 * 
	 * @param includeUnecessaryJoin you give us an EMPTY Set and we fill it with views that we had to join because of where expression.  For performance
	 * we do not do the other joins as the rows coming back have them directly
	 * @return
	 */
	public DirectCursor<IndexColumnInfo> getResultList(Set<ViewInfo> alreadyJoinedViews );

	/**
	 * The Iterable from getResults() is only loaded with 'batchSize' at a time from the nosql store so as you iterate
	 * GC should be releasing memory for the previous 500 while the Iterable loads the next 500.
	 * 
	 * @param batchSize
	 */
	public void setBatchSize(int batchSize);

}
