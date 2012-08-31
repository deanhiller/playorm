package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.util.AbstractCursor;

public class TypedResponseIter<T> extends AbstractCursor<KeyValue<TypedRow<T>>> {

	private DboTableMeta meta;
	private Iterable<T> keysIterable;
	private Iterator<T> keys;
	private Iterable<KeyValue<Row>> rowsIterable;
	private Iterator<KeyValue<Row>> rows;
	private String query;

	public TypedResponseIter(DboTableMeta meta, Iterable<T> keys,
			Iterable<KeyValue<Row>> rows) {
		this.meta = meta;
		this.keysIterable = keys;
		this.rowsIterable = rows;
		beforeFirst();
	}

	public TypedResponseIter(DboTableMeta meta2, Iterable<KeyValue<Row>> rows2,
			String query) {
		this.meta = meta2;
		this.rowsIterable = rows2;
		this.query = query;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		rows = rowsIterable.iterator();
		if(keysIterable != null)
			keys = keysIterable.iterator();
	}

	@Override
	protected com.alvazan.orm.util.AbstractCursor.Holder<KeyValue<TypedRow<T>>> nextImpl() {
		if(!rows.hasNext())
			return null;
		KeyValue<TypedRow<T>> val = nextChunk();
		return new Holder<KeyValue<TypedRow<T>>>(val);
	}
	
	private KeyValue<TypedRow<T>> nextChunk() {
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

}
