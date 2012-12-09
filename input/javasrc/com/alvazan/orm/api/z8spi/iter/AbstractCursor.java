package com.alvazan.orm.api.z8spi.iter;


public abstract class AbstractCursor<T> implements DirectCursor<T>,Cursor<T> {

	private Holder<T> currentValue;

	@Override
	public final boolean next() {
		currentValue = nextImpl();
		if(currentValue == null) {
			//There is NO next value so screw it, return false
			return false;
		}
		return true;
	}

	public abstract Holder<T> nextImpl();

	@Override
	public final T getCurrent() {
		if(currentValue == null)
			throw new IllegalArgumentException("There is no value at this position");
		return currentValue.getValue();
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
