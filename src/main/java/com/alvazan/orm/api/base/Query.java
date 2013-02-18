package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;

public interface Query<T> {

	public Query<T> setParameter(String name, Object value);
	
	/**
	 * If there is an entity in your query result list where the index has the value and
	 * the nosql store has no entity, KeyValue contains the exception delaying
	 * the exception until code accesses that missing value.  In this method, if you only
	 * iterate through the first 4 elements and the missing element was #5, you will not see
	 * any exception at all and code will keep working.
	 * 
	 * Also, you can call getException instead of getValue to avoid the Exception
	 * @return A <code>Cursor</code> 
	 */
	public Cursor<KeyValue<T>> getResults();
	/**
	 * getResults caches results returned as you iterate over them BUT if you are going to stream 1000's of results
	 * to someone, you an skip caching results so memory does not build up using this method...
	 * @param cacheResults 
	 * @return A <code>Cursor</code> 
	 */
	public Cursor<KeyValue<T>> getResults(boolean cacheResults);

	/**
	 * Only done when you have queries on a subclass in inheritance tables.  Then if you do a select all, you probably should
	 * supply the index to read from to select all the entries.
	 * @param indexedColumn
	 * @return A <code>Cursor</code> 
	 */
	public Cursor<KeyValue<T>> getResults(String indexedColumn);
	
	/**
	 * Wraps the cursor in a nice hasNext/next Iterable
	 * @return An iterator over the elements in the result
	 */
	public Iterable<KeyValue<T>> getResultsIter();
	
	/**
	 * getResults caches results returned as you iterate over them BUT if you are going to stream 1000's of results
	 * to someone, you can skip caching results so memory does not build up using this method...
	 * @param cacheResults  
	 * @return An iterator over the elements in the result
	 */
	public Iterable<KeyValue<T>> getResultsIter(boolean cacheResults);
	
	public T getSingleObject();
	
	/**
	 * WARNING: If you want to keep getting page after page, USE getResults method instead as that can iterate
	 * over trillions of rows without blowing out memory(BUT of course you should really only be iterating over 1 million
	 * or less rows to stay efficient and fast)
	 * 
	 * You probably should use getResultKeyValueList instead since that will delay exceptions caused by entities
	 * do not exist but are in the index (this is nosql after all)
	 * @param firstResult 0 or larger
	 * @param maxResults null if you want all the results though you should probably cap this so you don't blow out memory
	 * or use getResults method instead.
	 * 
	 * @return List of the entities
	 */
	public List<T> getResultList(int firstResult, Integer maxResults);
	
	/**
	 * The rate at which we pull from the nosql store.  The default is 500.  We grab 500 entities
	 * and once you hit 501, we grab the next 500 and so on so you can discard entities as you iterate
	 * over them.
	 * 
	 * @param batchSize
	 */
	public void setBatchSize(int batchSize);
}
