package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class IterRowProxy<T> implements Iterable<KeyValue<T>>{

	private MetaClass<T> meta;
	private Iterable<KeyValue<Row>> rows;
	private NoSqlSession session;
	private boolean alreadyRan = false;
	
	public IterRowProxy(MetaClass<T> meta, Iterable<KeyValue<Row>> rows, NoSqlSession s) {
		this.meta = meta;
		this.rows = rows;
		this.session = s;
	}

	@Override
	public Iterator<KeyValue<T>> iterator() {
		if(alreadyRan)
			throw new IllegalStateException("Sorry, you cannot run this iterable twice!!!! as then we would end up RE-translating all your entities so instead use the results of the first time you iterated over it");
		alreadyRan = true;
		return new IteratorRowProxy<T>(meta, rows.iterator(), session);
	}
	
	private static class IteratorRowProxy<T> implements Iterator<KeyValue<T>> {

		private MetaClass<T> meta;
		private Iterator<KeyValue<Row>> iterator;
		private NoSqlSession session;

		public IteratorRowProxy(MetaClass<T> meta, Iterator<KeyValue<Row>> iterator, NoSqlSession s) {
			this.meta = meta;
			this.iterator = iterator;
			this.session = s;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public KeyValue<T> next() {
			KeyValue<Row> kv = iterator.next();
			Row row = kv.getValue();
			Object key = kv.getKey();
			
			KeyValue<T> keyVal;
			if(row == null) {
				keyVal = new KeyValue<T>();
				keyVal.setKey(key);
			} else {
				keyVal = meta.translateFromRow(row, session);
			}
			
			return keyVal;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	

}
