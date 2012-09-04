package com.alvazan.orm.util;

import java.util.Iterator;

public abstract class AbstractIterable<T> implements Iterable<T> {

	public AbstractIterator<T> iteratorAbstract() {
		return (AbstractIterator<T>) iterator();
	}

	@Override
	public abstract Iterator<T> iterator();

}
