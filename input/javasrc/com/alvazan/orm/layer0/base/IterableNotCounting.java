package com.alvazan.orm.layer0.base;

import java.util.Iterator;

public class IterableNotCounting implements Iterable<byte[]> {

	private Iterator<byte[]> keysIterator;
	private boolean alreadyRun = false;
	
	public IterableNotCounting(Iterator<byte[]> keysIterator) {
		this.keysIterator = keysIterator;
	}

	@Override
	public Iterator<byte[]> iterator() {
		if(alreadyRun)
			throw new IllegalStateException("BUG, This iterable can only be run once as it is proxy to an Iterator that CAN ONLY be run once");
		alreadyRun = true;
		return new EmptyIterator(keysIterator);
	}
	private static class EmptyIterator implements Iterator<byte[]> {

		private Iterator<byte[]> keysIterator;

		public EmptyIterator(Iterator<byte[]> keysIterator) {
			this.keysIterator = keysIterator;
		}

		@Override
		public boolean hasNext() {
			return keysIterator.hasNext();
		}

		@Override
		public byte[] next() {
			return keysIterator.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported, bug if you see this");
		}
		
	}

}
