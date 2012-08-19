package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class IterRowProxy<T> implements Iterable<KeyValue<T>>{

	private MetaClass<T> meta;
	private Iterable<KeyValue<Row>> rows;
	private NoSqlSession session;
	private boolean alreadyRan = false;
	private String query;
	
	public IterRowProxy(MetaClass<T> meta, Iterable<KeyValue<Row>> rows, NoSqlSession s, String query2) {
		this.meta = meta;
		this.rows = rows;
		this.session = s;
		this.query = query2;
	}

	@Override
	public Iterator<KeyValue<T>> iterator() {
		if(alreadyRan)
			throw new IllegalStateException("Sorry, you cannot run this iterable twice YET!!!! currently it would RE-translate all...we need to implement caching so retranslate does not happen");
		alreadyRan = true;
		return new IteratorRowProxy<T>(meta, rows.iterator(), session, query);
	}
	
	private static class IteratorRowProxy<T> implements Iterator<KeyValue<T>> {

		private MetaClass<T> meta;
		private Iterator<KeyValue<Row>> iterator;
		private NoSqlSession session;
		private String query;
		public IteratorRowProxy(MetaClass<T> meta, Iterator<KeyValue<Row>> iterator, NoSqlSession s, String query) {
			this.meta = meta;
			this.iterator = iterator;
			this.session = s;
			this.query = query;
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
			// TODO Auto-generated method stub
			
		}
		
	}
	
	

}
