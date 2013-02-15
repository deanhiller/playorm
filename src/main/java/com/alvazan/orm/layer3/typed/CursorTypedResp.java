package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;

public class CursorTypedResp<T> extends AbstractCursor<KeyValue<TypedRow>> {

	private DboTableMeta meta;
	private Iterable<T> keysIterable;
	private Iterator<T> keys;
	private AbstractCursor<KeyValue<Row>> rowsIterable;
	private String query;

	public CursorTypedResp(DboTableMeta meta, Iterable<T> keys,
			AbstractCursor<KeyValue<Row>> rows) {
		Precondition.check(meta, "meta");
		Precondition.check(rows, "rows");
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
	public void afterLast() {
		rowsIterable.afterLast();
		if(keysIterable != null)
			throw new UnsupportedOperationException("Reverse iteration is not yet supported for this curor type.  Try sending in your keys in reverse order");
	}

	@Override
	public Holder<KeyValue<TypedRow>> nextImpl() {
		Holder<KeyValue<Row>> nextImpl = rowsIterable.nextImpl();
		if(nextImpl == null)
			return null;
		
		KeyValue<TypedRow> val = nextChunk(nextImpl.getValue());
		return new Holder<KeyValue<TypedRow>>(val);
	}
	
	@Override
	public Holder<KeyValue<TypedRow>> previousImpl() {
		Holder<KeyValue<Row>> previousImpl = rowsIterable.previousImpl();
		if(previousImpl == null)
			return null;
		
		KeyValue<TypedRow> val = prevChunk(previousImpl.getValue());
		return new Holder<KeyValue<TypedRow>>(val);
	}
	
	private KeyValue<TypedRow> nextChunk(KeyValue<Row> keyValue) {
		if(query == null) {
			return nextVal(keyValue);
		}
		return nextForQuery(keyValue);
	}
	
	private KeyValue<TypedRow> prevChunk(KeyValue<Row> keyValue) {
		if(query == null) {
			throw new UnsupportedOperationException("Reverse iteration is not yet supported for this curor type.  Try sending in your keys in reverse order");
		}
		return nextForQuery(keyValue);
	}

	@SuppressWarnings("unchecked")
	private KeyValue<TypedRow> nextForQuery(KeyValue<Row> kv) {
		Row row = kv.getValue();
		byte[] virtualKey = (byte[]) kv.getKey();
		DboColumnIdMeta idField = meta.getIdColumnMeta();
		
		KeyValue<TypedRow> keyVal;
		if(row == null) {
			byte[] notVirtKey = idField.unformVirtRowKey(virtualKey);
			T key = (T) idField.convertFromStorage2(notVirtKey);
			keyVal = new KeyValue<TypedRow>();
			keyVal.setKey(key);
			RowNotFoundException exc = new RowNotFoundException("Your query="+query+" contained a value with a pk where that entity no longer exists in the nosql store");
			keyVal.setException(exc);
		} else {
			keyVal = meta.translateFromRow(row);
		}
		
		return keyVal;
	}
	
	
	private KeyValue<TypedRow> nextVal(KeyValue<Row> kv) {
		Row row = kv.getValue();
		T key = keys.next();
		
		KeyValue<TypedRow> keyVal;
		if(row == null) {
			keyVal = new KeyValue<TypedRow>();
			keyVal.setKey(key);
		} else {
			keyVal = meta.translateFromRow(row);
		}

		return keyVal;
	}

}
