package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.impl.meta.data.MetaClass;

public class IterableKey<T> implements Iterable<byte[]> {

	private MetaClass<T> meta;
	private Iterable<? extends Object> keys;

	public IterableKey(MetaClass<T> meta, Iterable<? extends Object> keys) {
		this.meta = meta;
		this.keys = keys;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IteratorProxy<T>(meta, keys.iterator());
	}
	
	private static class IteratorProxy<T> implements Iterator<byte[]> {

		private MetaClass<T> meta;
		private Iterator<? extends Object> iterator;

		public IteratorProxy(MetaClass<T> meta,
				Iterator<? extends Object> iterator) {
			this.meta = meta;
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public byte[] next() {
			Object next = iterator.next();
			byte[] key = meta.convertIdToNoSql(next);
			return key;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}

}
