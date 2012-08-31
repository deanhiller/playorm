package com.alvazan.orm.util;

import com.alvazan.orm.api.base.Cursor;

public abstract class AbstractCursor<T> implements Cursor<T>{

	private Holder<T> nextValue;

	@Override
	public final boolean hasNext() {
		if(nextValue != null) {
			//DO NOT get nextImpl yet since he has not gotten the current one so 
			//just keep returning true instead!!!
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

	protected <C> Holder<C> nextFromCursor(Cursor<C> cursor2) {
		AbstractCursor<C> c = (AbstractCursor<C>) cursor2;
		return c.nextImpl();
	}
	
	protected abstract Holder<T> nextImpl();

	@Override
	public final T next() {
		Holder<T> temp = nextValue;
		if(temp == null) {
			//Here, they may call next to get the first element WITHOUT calling hasNext, so we call hasNext
			if(!hasNext())
				throw new IllegalArgumentException("This cursor has run out of value.  Call hasNext() to avoid this exception and check before calling next()!!!!");
			temp = nextValue;
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
