package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.AbstractIterator;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class CursorRow<T> extends AbstractCursor<KeyValue<T>>{

	private MetaClass<T> meta;
	private NoSqlSession session;
	private String query;
	private Integer batchSize;
	
	private Iterable<byte[]> noSqlKeys;
	private AbstractIterator<byte[]> keysIterator;
	private AbstractCursor<KeyValue<Row>> lastCachedRows;
	private int numberOfRows;
	
	public CursorRow(MetaClass<T> meta, Iterable<byte[]> noSqlKeys, NoSqlSession s, String query2, Integer batchSize) {
		Precondition.check(noSqlKeys, "noSqlKeys");
		Precondition.check(meta, "meta");
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
			else if(batchSize == null) //If batchSize was null, we fetched them all already
				return null;
			else if(numberOfRows < batchSize) //If we did not read in a full batch last time, we are done
				return null;
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
		numberOfRows = 0;
		Holder<KeyValue<Row>> nextImpl = lastCachedRows.nextImpl();
		if(nextImpl == null)
			return null;
		return translateRow(nextImpl.getValue());
	}

	private KeyValue<T> translateRow(KeyValue<Row> kv) {
		numberOfRows++;
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
