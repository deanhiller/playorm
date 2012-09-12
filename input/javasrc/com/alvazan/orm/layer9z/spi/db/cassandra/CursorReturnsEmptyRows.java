package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;

public class CursorReturnsEmptyRows extends AbstractCursor<KeyValue<Row>> {

	private Iterable<byte[]> keysIterable;
	private Iterator<byte[]> keys;
	public CursorReturnsEmptyRows(Iterable<byte[]> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys cannot be null");
		this.keysIterable = keys;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		keys = keysIterable.iterator();
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> nextImpl() {
		if(!keys.hasNext())
			return null;
		byte[] key = keys.next();
		KeyValue<Row> kv = new KeyValue<Row>();
		kv.setKey(key);
		
		return new Holder<KeyValue<Row>>(kv);		
	}
}
