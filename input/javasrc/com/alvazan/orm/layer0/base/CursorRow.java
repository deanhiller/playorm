package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.AbstractCursor;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.util.AbstractIterator;

public class CursorRow<T> extends AbstractCursor<KeyValue<T>>{

	private MetaClass<T> meta;
	private NoSqlSession session;
	private String query;
	private Integer batchSize;
	
	private Iterable<byte[]> noSqlKeys;
	private AbstractIterator<byte[]> keysIterator;
	private AbstractCursor<KeyValue<Row>> lastCachedRows;
	
	public CursorRow(MetaClass<T> meta, Iterable<byte[]> noSqlKeys, NoSqlSession s, String query2, Integer batchSize) {
		this.meta = meta;
		this.noSqlKeys = noSqlKeys;
		this.session = s;
		this.query = query2;
		this.batchSize = batchSize;
		beforeFirst();
	}
	
	@Override
	public void beforeFirst() {
		Iterator<byte[]> temp = noSqlKeys.iterator();
		if(!(temp instanceof AbstractIterator)) {
			keysIterator = new IterProxy(temp);
		} else
			keysIterator = (AbstractIterator<byte[]>) temp;
	}
	
	@Override
	public Holder<KeyValue<T>> nextImpl() {
		KeyValue<T> nextResult = fetchNextResult();
		if(nextResult == null)
			return null;
		
		return new Holder<KeyValue<T>>(nextResult);
	}
	
	private KeyValue<T> fetchNextResult() {
		if(lastCachedRows != null) {
			Holder<KeyValue<Row>> holder = lastCachedRows.nextImpl();
			if(holder != null)
				return translateRow(holder.getValue());
			//we need to fetch more
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
		lastCachedRows = session.findAll(cf, proxyCounting, skipCache);
		Holder<KeyValue<Row>> nextImpl = lastCachedRows.nextImpl();
		if(nextImpl == null)
			return null;
		return translateRow(nextImpl.getValue());
	}

	private KeyValue<T> translateRow(KeyValue<Row> kv) {
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
