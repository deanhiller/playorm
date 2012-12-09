package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;

public class IterableKey<T> implements Iterable<byte[]> {

	private MetaIdField<T> meta;
	private Iterable<? extends Object> keys;

	public IterableKey(MetaClass<T> meta, Iterable<? extends Object> keys) {
		Precondition.check(meta,"meta");
		Precondition.check(keys,"keys");
		this.meta = meta.getIdField();
		this.keys = keys;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IteratorProxy<T>(meta, keys.iterator());
	}
	
	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "IterableKey(getNextKey)["+
				tabs+"idMeta="+meta+
				tabs+"iterable="+keys+
				tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	private static class IteratorProxy<T> implements Iterator<byte[]> {


		private Iterator<? extends Object> iterator;
		private MetaIdField<T> idMeta;

		public IteratorProxy(MetaIdField<T> meta,
				Iterator<? extends Object> iterator) {
			this.idMeta = meta;
			this.iterator = iterator;
		}
		
		@Override
		public String toString() {
			String tabs = StringLocal.getAndAdd();
			String retVal = "IteratorProxy(getNextKey)["+
					tabs+"idMeta="+idMeta+
					tabs+"iterator="+iterator+
					tabs+"]";
			StringLocal.set(tabs.length());
			return retVal;
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
