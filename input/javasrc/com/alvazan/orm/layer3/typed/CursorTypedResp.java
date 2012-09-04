package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;

public class CursorTypedResp<T> extends AbstractCursor<KeyValue<TypedRow<T>>> {

	private DboTableMeta meta;
	private Iterable<T> keysIterable;
	private Iterator<T> keys;
	private AbstractCursor<KeyValue<Row>> rowsIterable;
	private String query;

	public CursorTypedResp(DboTableMeta meta, Iterable<T> keys,
			AbstractCursor<KeyValue<Row>> rows) {
		this.meta = meta;
		this.keysIterable = keys;
		this.rowsIterable = rows;
		beforeFirst();
	}

	public CursorTypedResp(DboTableMeta meta2, AbstractCursor<KeyValue<Row>> rows2,
			String query) {
		this.meta = meta2;
		this.rowsIterable = rows2;
		this.query = query;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		rowsIterable.beforeFirst();
		if(keysIterable != null)
			keys = keysIterable.iterator();
	}

	@Override
	public Holder<KeyValue<TypedRow<T>>> nextImpl() {
		Holder<KeyValue<Row>> nextImpl = rowsIterable.nextImpl();
		if(nextImpl == null)
			return null;
		
		KeyValue<TypedRow<T>> val = nextChunk(nextImpl.getValue());
		return new Holder<KeyValue<TypedRow<T>>>(val);
	}
	
	private KeyValue<TypedRow<T>> nextChunk(KeyValue<Row> keyValue) {
		if(query == null) {
			return nextVal(keyValue);
		}
		return nextForQuery(keyValue);
	}

	@SuppressWarnings("unchecked")
	private KeyValue<TypedRow<T>> nextForQuery(KeyValue<Row> kv) {
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

	private KeyValue<TypedRow<T>> nextVal(KeyValue<Row> kv) {
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
