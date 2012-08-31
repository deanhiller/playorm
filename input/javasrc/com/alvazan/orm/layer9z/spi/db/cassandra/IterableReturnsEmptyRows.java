package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;

public class IterableReturnsEmptyRows implements Iterable<KeyValue<Row>> {

	private Iterable<byte[]> keys;

	public IterableReturnsEmptyRows(Iterable<byte[]> keys) {
		this.keys = keys;
	}

	@Override
	public Iterator<KeyValue<Row>> iterator() {
		return new IteratorEmpty(keys.iterator());
	}
	
	private static class IteratorEmpty implements Iterator<KeyValue<Row>> {

		private Iterator<byte[]> keys;

		public IteratorEmpty(Iterator<byte[]> keys) {
			this.keys = keys;
		}

		@Override
		public boolean hasNext() {
			return keys.hasNext();
		}

		@Override
		public KeyValue<Row> next() {
			byte[] key = keys.next();
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(key);
			
			return kv;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported, never will be");
		}
		
		
	}

}
