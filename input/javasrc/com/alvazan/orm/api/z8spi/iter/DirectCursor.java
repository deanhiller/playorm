package com.alvazan.orm.api.z8spi.iter;

import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;

public interface DirectCursor<T> {

	public Holder<T> nextImpl();

	public void beforeFirst();
	
}
