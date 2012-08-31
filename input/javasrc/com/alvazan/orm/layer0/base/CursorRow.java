package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.util.AbstractCursor;

public class CursorRow<T> extends AbstractCursor<KeyValue<T>>{

	private MetaClass<T> meta;
	private NoSqlSession session;
	private String query;
	private Integer batchSize;
	
	private Iterator<byte[]> keysIterator;
	private Iterator<KeyValue<Row>> lastCachedRows;
	
	public CursorRow(MetaClass<T> meta, Iterable<byte[]> noSqlKeys, NoSqlSession s, String query2, Integer batchSize) {
		this.meta = meta;
		this.keysIterator = noSqlKeys.iterator();
		this.session = s;
		this.query = query2;
		this.batchSize = batchSize;
	}

	@Override
	protected Holder<KeyValue<T>> nextImpl() {
		fetchMoreResults();
		if(lastCachedRows == null)
			return null;
		
		KeyValue<T> row = fetchRow();
		return new Holder<KeyValue<T>>(row);
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
	
}
