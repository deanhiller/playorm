package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class IterableRow<T> implements Iterable<KeyValue<T>>{

	private MetaClass<T> meta;
	private Iterable<byte[]> noSqlKeys;
	private NoSqlSession session;
	private boolean alreadyRan = false;
	private String query;
	private Integer batchSize;
	
	public IterableRow(MetaClass<T> meta, Iterable<byte[]> noSqlKeys, NoSqlSession s, String query2, Integer batchSize) {
		this.meta = meta;
		this.noSqlKeys = noSqlKeys;
		this.session = s;
		this.query = query2;
		this.batchSize = batchSize;
	}

	@Override
	public Iterator<KeyValue<T>> iterator() {
		if(alreadyRan)
			throw new IllegalStateException("Sorry, you cannot run this iterable twice YET!!!! currently it would RE-translate all..." +
					"we need to implement caching so retranslate does not happen.  In most cases, it would also RE-fetch " +
					"data from the database which we definitely don't want either.  Also, the iterable could be a proxy into " +
					"millions of rows so do you really want to iterate twice?");
		alreadyRan = true;
		return new IteratorRowProxy<T>(meta, noSqlKeys.iterator(), session, query, batchSize);
	}
	
	private static class IteratorRowProxy<T> implements Iterator<KeyValue<T>> {

		private MetaClass<T> meta;
		private Iterator<byte[]> keysIterator;
		private NoSqlSession session;
		private String query;
		private Integer batchSize;
		private Iterator<KeyValue<Row>> lastCachedRows;

		public IteratorRowProxy(MetaClass<T> meta, Iterator<byte[]> keys, NoSqlSession s, String query, Integer batchSize) {
			this.meta = meta;
			this.keysIterator = keys;
			this.session = s;
			this.query = query;
			this.batchSize = batchSize;
		}

		@Override
		public boolean hasNext() {
			fetchMoreResults();
			if(lastCachedRows == null)
				return false;
			
			boolean has = lastCachedRows.hasNext();
			return has;
		}

		private void fetchMoreResults() {
			if(lastCachedRows != null && lastCachedRows.hasNext())
				return; //We have rows left so do NOT fetch results
			else if(!keysIterator.hasNext()) {
				//we know we have NO rows left in cache at this point but there are als NO 
				//keys left!!! so return and set the lastCachedRows to null
				lastCachedRows = null;
				return;
			}
			
			
			String cf = meta.getColumnFamily();		
			//If batchSize is null, we MUST use the keysIterator not iterable(just trust me or the keyIterator.hasNext up above no longer works)
			Iterable<byte[]> proxyCounting = new IterableNotCounting(this.keysIterator);
			if(batchSize != null) {
				//let's give a window of batchSize view into the iterator ;)  so it will stop at 
				//batchSize when looping over our iterator
				proxyCounting = new IterableCounting(this.keysIterator, batchSize);
			}
			boolean skipCache = query != null; //if someone is querying into, we need to skip the cache!!!
			Iterable<KeyValue<Row>> rows = session.findAll(cf, proxyCounting, skipCache);
			lastCachedRows = rows.iterator();
		}

		@Override
		public KeyValue<T> next() {
			fetchMoreResults();
			if(lastCachedRows == null)
				throw new IllegalStateException("There is no more data...try to call iterator.hasNext first so this doesn't happen ;) ");
			
			return fetchRow();
		}

		private KeyValue<T> fetchRow() {
			KeyValue<Row> kv = lastCachedRows.next();
			Row row = kv.getValue();
			Object key = kv.getKey();
			
			KeyValue<T> keyVal;
			if(row == null) {
				keyVal = new KeyValue<T>();
				Object obj = meta.getIdField().translateFromBytes((byte[]) key);
				if(query != null) {
					RowNotFoundException exc = new RowNotFoundException("Your query="+query+" contained a value with a pk where that entity no longer exists in the nosql store");
					keyVal.setException(exc);
				}
				keyVal.setKey(obj);
			} else {
				keyVal = meta.translateFromRow(row, session);
			}
			
			return keyVal;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and probably never will be so must be bug");
		}
		
	}
	
	

}
