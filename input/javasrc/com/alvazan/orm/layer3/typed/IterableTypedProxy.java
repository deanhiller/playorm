package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class IterableTypedProxy<T> implements Iterable<byte[]> {

	private DboColumnMeta idMeta;
	private Iterable<T> keys;
	
	public IterableTypedProxy(DboColumnMeta idMeta, Iterable<T> keys2) {
		Precondition.check(keys2, "keys");
		this.idMeta = idMeta;
		this.keys = keys2;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new TypedIteratorProxy<T>(idMeta, keys.iterator());
	}
	
	private static class TypedIteratorProxy<T> implements Iterator<byte[]> {

		private DboColumnMeta idMeta;
		private Iterator<T> iterator;

		public TypedIteratorProxy(DboColumnMeta idMeta, Iterator<T> iterator) {
			this.idMeta = idMeta;
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public byte[] next() {
			T k = iterator.next();
			byte[] rowK = idMeta.convertToStorage2(k);
			return rowK;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and won't ever be");
		}
	}

}
