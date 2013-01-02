package com.alvazan.orm.layer9z.spi.db.cassandra;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;

public class CursorReturnsEmptyRows extends AbstractCursor<KeyValue<Row>> {

	private DirectCursor<byte[]> keys;
	public CursorReturnsEmptyRows(DirectCursor<byte[]> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys cannot be null");
		this.keys = keys;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		keys.beforeFirst();
	}
	
	@Override
	public void afterLast() {
		keys.afterLast();
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> nextImpl() {
		
		byte[] key = keys.nextImpl().getValue();
		KeyValue<Row> kv = new KeyValue<Row>();
		kv.setKey(key);
		
		return new Holder<KeyValue<Row>>(kv);		
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> previousImpl() {
		byte[] key = keys.previousImpl().getValue();
		KeyValue<Row> kv = new KeyValue<Row>();
		kv.setKey(key);
		
		return new Holder<KeyValue<Row>>(kv);			
	}
}
