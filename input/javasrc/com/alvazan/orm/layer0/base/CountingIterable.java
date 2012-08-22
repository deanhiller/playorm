package com.alvazan.orm.layer0.base;

import java.util.Iterator;

public class CountingIterable implements Iterable<byte[]> {

	private Iterator<byte[]> keysIterator;
	private boolean alreadyRun = false;
	private int batchSize;
	public CountingIterable(Iterator<byte[]> keysIterator, int batchSize2) {
		this.keysIterator = keysIterator;
		this.batchSize = batchSize2;
	}

	@Override
	public Iterator<byte[]> iterator() {
		if(alreadyRun)
			throw new IllegalStateException("BUG, This iterable can only be run once as it is proxy to an Iterator that CAN ONLY be run once");
		alreadyRun = true;
		return new CountingIterator(keysIterator, batchSize);
	}

	private static class CountingIterator implements Iterator<byte[]>{

		private Iterator<byte[]> keysIterator;
		private int count = 0;
		private int batchSize;
		public CountingIterator(Iterator<byte[]> keysIterator, int batchSize2) {
			this.keysIterator = keysIterator;
			this.batchSize = batchSize2;
		}

		@Override
		public boolean hasNext() {
			if(count < batchSize && keysIterator.hasNext())
				return true;
			return false;
		}

		@Override
		public byte[] next() {
			if(count < batchSize && keysIterator.hasNext()) {
				return keysIterator.next();
			} else
				throw new IllegalArgumentException("You should really be calling iterator.hasNext before this method or you get this exception BECAUSE this iterator has run out of values");
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and probably will be so this is a bug if you see this");
		}
	}
}
