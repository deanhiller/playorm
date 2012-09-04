package com.alvazan.orm.util;

import java.util.Iterator;

public abstract class AbstractIterator<T> implements Iterator<T>{

	private IterHolder<T> nextValue;

	@Override
	public final boolean hasNext() {
		if(nextValue != null) {
			//DO NOT get nextImpl yet since he has not called next yet to clear out the next value
			//field so just keep returning true instead!!!
			return true;
		}
		//Okay, let's get the next value here and see if there actually is one
		nextValue = nextImpl();
		if(nextValue == null) {
			//There is NO next value so screw it, return false
			return false;
		}
		return true;
	}

	public final IterHolder<T> nextImpl() {
		//check for cached value first to make sure(we had a case where one piece called hasNext and down below
		//someone called nextImpl which skipped the cached value
		if(nextValue == null)
			return nextImpl2();
		IterHolder<T> temp = nextValue;
		nextValue = null;
		return temp;
	}
	
	protected abstract IterHolder<T> nextImpl2();

	@Override
	public final T next() {
		IterHolder<T> temp = nextValue;
		if(temp == null) {
			//Here, they may call next to get the first element WITHOUT calling hasNext, so we call hasNext
			if(!hasNext())
				throw new IllegalArgumentException("This cursor has run out of value.  Call hasNext() to avoid this exception and check before calling next()!!!!");
			temp = nextValue;
		}
		nextValue = null;
		return temp.getValue();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("not supported");
	}
	
	public static class IterHolder<T> {
		private T value;
		public IterHolder(T val) {
			this.value = val;
		}
		public T getValue() {
			return value;
		}
	}
}
