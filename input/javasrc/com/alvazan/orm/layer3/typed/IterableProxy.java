package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;

public class IterableProxy<T> implements
		Iterable<com.alvazan.orm.api.z8spi.KeyValue<T>> {

	private Cursor<KeyValue<T>> cursor;

	public IterableProxy(Cursor<KeyValue<T>> cursor) {
		this.cursor = cursor;
	}

	@Override
	public Iterator<KeyValue<T>> iterator() {
		//restart cursor
		cursor.beforeFirst();
		return new IteratorProxy<T>(cursor);
	}
	
	private static class IteratorProxy<T> implements Iterator<KeyValue<T>> {

		private Cursor<KeyValue<T>> cursor;
		private KeyValue<T> cachedValue;
		
		public IteratorProxy(Cursor<KeyValue<T>> cursor) {
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			if(cachedValue != null)
				return true;
			boolean hasNext = cursor.next();
			if(hasNext) 
				cachedValue = cursor.getCurrent();
			return hasNext;
		}

		@Override
		public KeyValue<T> next() {
			if(!hasNext())
				throw new IllegalStateException("You should call hasNext first!!! This iterator has no more values");
			
			KeyValue<T> temp = cachedValue;
			cachedValue = null;
			return temp;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}
}
