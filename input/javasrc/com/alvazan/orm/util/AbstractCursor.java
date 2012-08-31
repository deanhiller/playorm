package com.alvazan.orm.util;

import com.alvazan.orm.api.base.Cursor;

public abstract class AbstractCursor<T> implements Cursor<T>{

	private Holder<T> nextValue;

	@Override
	public final boolean hasNext() {
		nextValue = nextImpl();
		if(nextValue == null)
			return false;
		return true;
	}

	protected abstract Holder<T> nextImpl();

	@Override
	public final T next() {
		Holder<T> temp = nextValue;
		if(temp == null) {
			temp = nextImpl();
			if(temp == null)
				throw new IllegalArgumentException("This cursor has run out of value.  Call hasNext() to avoid this exception and check before calling next()!!!!");
		}
		nextValue = null;
		return temp.getValue();
	}

	public static class Holder<T> {
		private T value;
		public Holder(T val) {
			this.value = val;
		}
		public T getValue() {
			return value;
		}
	}
}
