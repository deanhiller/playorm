package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;

public class IterableKey<T> implements Iterable<byte[]> {

	private MetaClass<T> meta;
	private Iterable<? extends Object> keys;

	public IterableKey(MetaClass<T> meta, Iterable<? extends Object> keys) {
		Precondition.check(meta,"meta");
		Precondition.check(keys,"keys");
		this.meta = meta;
		this.keys = keys;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IteratorProxy<T>(meta, keys.iterator());
	}
	
	private static class IteratorProxy<T> implements Iterator<byte[]> {


		private Iterator<? extends Object> iterator;
		private MetaIdField<T> idMeta;

		public IteratorProxy(MetaClass<T> meta,
				Iterator<? extends Object> iterator) {
			this.idMeta = meta.getIdField();
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public byte[] next() {
			Object next = iterator.next();
			byte[] nonVirtKey = idMeta.convertIdToNonVirtKey(next);
			if(nonVirtKey == null)
				throw new IllegalArgumentException("You supplied a null key to your list when calling findAll method.  We can't lookup null as a key");
			//NOTE: Next iterator CONVERTS to virtual key so do not do it here!!!!
			return nonVirtKey;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}
}
