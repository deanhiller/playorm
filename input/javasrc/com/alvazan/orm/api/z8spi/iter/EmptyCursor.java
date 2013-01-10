package com.alvazan.orm.api.z8spi.iter;


public class EmptyCursor<T> extends AbstractCursor<T> {

	@Override
	public void beforeFirst() {
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> nextImpl() {
		return null;
	}
	
	@Override
	public void afterLast() {
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> previousImpl() {
		return null;
	}


}
