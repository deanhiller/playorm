package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.spi3.TypedRow;
import com.alvazan.orm.api.spi3.meta.DboColumnIdMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;

public class TypedResponseIter<T> implements Iterable<KeyValue<TypedRow<T>>> {

	private DboTableMeta meta;
	private Iterable<T> keys;
	private Iterable<KeyValue<Row>> rows;
	private String query;

	public TypedResponseIter(DboTableMeta meta, Iterable<T> keys,
			Iterable<KeyValue<Row>> rows) {
		this.meta = meta;
		this.keys = keys;
		this.rows = rows;
	}

	public TypedResponseIter(DboTableMeta meta2, Iterable<KeyValue<Row>> rows2,
			String query) {
		this.meta = meta2;
		this.rows = rows2;
		this.query = query;
	}

	@Override
	public Iterator<KeyValue<TypedRow<T>>> iterator() {
		Iterator<T> keysIter = null;
		if(keys != null)
			keysIter = keys.iterator();
		return new TypedResponseIterator<T>(meta, keysIter, rows.iterator(), query);
	}
	
	private static class TypedResponseIterator<T> implements Iterator<KeyValue<TypedRow<T>>> {

		private DboTableMeta meta;
		private Iterator<T> keys;
		private Iterator<KeyValue<Row>> rows;
		private String query;

		public TypedResponseIterator(DboTableMeta meta, Iterator<T> keys,
				Iterator<KeyValue<Row>> rows, String query) {
			this.meta = meta;
			this.keys = keys;
			this.rows = rows;
			this.query = query;
		}

		@Override
		public boolean hasNext() {
			return rows.hasNext();
		}

		@Override
		public KeyValue<TypedRow<T>> next() {
			if(query == null) {
				return nextVal();
			}
			return nextForQuery();
		}

		@SuppressWarnings("unchecked")
		private KeyValue<TypedRow<T>> nextForQuery() {
			KeyValue<Row> kv = rows.next();
			Row row = kv.getValue();
			byte[] rowKey = (byte[]) kv.getKey();
			DboColumnIdMeta idField = meta.getIdColumnMeta();
			T key = (T) idField.convertFromStorage2(rowKey);
			
			KeyValue<TypedRow<T>> keyVal;
			if(row == null) {
				keyVal = new KeyValue<TypedRow<T>>();
				keyVal.setKey(key);
				RowNotFoundException exc = new RowNotFoundException("Your query="+query+" contained a value with a pk where that entity no longer exists in the nosql store");
				keyVal.setException(exc);
			} else {
				keyVal = meta.translateFromRow(row);
			}
			
			return keyVal;
		}

		private KeyValue<TypedRow<T>> nextVal() {
			KeyValue<Row> kv = rows.next();
			Row row = kv.getValue();
			T key = keys.next();
			
			KeyValue<TypedRow<T>> keyVal;
			if(row == null) {
				keyVal = new KeyValue<TypedRow<T>>();
				keyVal.setKey(key);
			} else {
				keyVal = meta.translateFromRow(row);
			}

			return keyVal;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported and probably never will be");
		}
	}
}
