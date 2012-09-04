package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.iter.AbstractIterator;

public class IterableNotCounting implements Iterable<byte[]> {

	private AbstractIterator<byte[]> keysIterator;
	private boolean alreadyRun = false;
	
	public IterableNotCounting(AbstractIterator<byte[]> keysIterator2) {
		this.keysIterator = keysIterator2;
	}

	@Override
	public Iterator<byte[]> iterator() {
		if(alreadyRun)
			throw new IllegalStateException("BUG, This iterable can only be run once as it is proxy to an Iterator that CAN ONLY be run once");
		alreadyRun = true;
		return new EmptyIterator(keysIterator);
	}
	private static class EmptyIterator extends AbstractIterator<byte[]> {
		private AbstractIterator<byte[]> keysIterator;

		public EmptyIterator(AbstractIterator<byte[]> keysIterator2) {
			this.keysIterator = keysIterator2;
		}
		@Override
		public IterHolder<byte[]> nextImpl() {
			return keysIterator.nextImpl();
		}
	}
}
