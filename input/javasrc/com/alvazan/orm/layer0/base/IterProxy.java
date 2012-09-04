package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.util.AbstractIterator;

public class IterProxy extends AbstractIterator<byte[]> {

	private Iterator<byte[]> iterator;

	public IterProxy(Iterator<byte[]> iter) {
		this.iterator = iter;
	}

	@Override
	protected com.alvazan.orm.util.AbstractIterator.IterHolder<byte[]> nextImpl2() {
		if(!iterator.hasNext())
			return null;
		return new IterHolder<byte[]>(iterator.next());
	}
}
