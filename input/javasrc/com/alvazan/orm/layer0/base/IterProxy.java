package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.util.AbstractIterator;

public class IterProxy extends AbstractIterator<byte[]> {

	private Iterator<byte[]> iterator;

	public IterProxy(Iterator<byte[]> iter) {
		this.iterator = iter;
	}

	@Override
	public com.alvazan.orm.util.AbstractIterator.IterHolder<byte[]> nextImpl() {
		if(!iterator.hasNext())
			return null;
		return new IterHolder<byte[]>(iterator.next());
	}
}
