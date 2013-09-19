package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;

public class CursorReturnsEmptyRows2 extends AbstractCursor<KeyValue<Row>> {

	private DirectCursor<byte[]> keys;
	public CursorReturnsEmptyRows2(DirectCursor<byte[]> keys) {
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
		Holder<byte[]> holder = keys.nextImpl();
		if (holder == null)
			return null;

		byte[] key = holder.getValue();
		KeyValue<Row> kv = new KeyValue<Row>();
		kv.setKey(key);
		
		return new Holder<KeyValue<Row>>(kv);		
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> previousImpl() {
		Holder<byte[]> holder = keys.previousImpl();
		if (holder == null)
			return null;

		byte[] key = holder.getValue();
		KeyValue<Row> kv = new KeyValue<Row>();
		kv.setKey(key);
		
		return new Holder<KeyValue<Row>>(kv);			
	}
}
