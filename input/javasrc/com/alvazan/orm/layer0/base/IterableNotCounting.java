package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.util.AbstractIterable;
import com.alvazan.orm.util.AbstractIterator;

public class IterableNotCounting extends AbstractIterable<byte[]> {

	private Iterator<byte[]> keysIterator;
	private boolean alreadyRun = false;
	
	public IterableNotCounting(Iterator<byte[]> keysIterator2) {
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
		private Iterator<byte[]> keysIterator;

		public EmptyIterator(Iterator<byte[]> keysIterator2) {
			this.keysIterator = keysIterator2;
		}
		@Override
		public IterHolder<byte[]> nextImpl2() {
			if(!keysIterator.hasNext())
				return null;
			byte[] data = keysIterator.next();
			return new IterHolder<byte[]>(data);
		}
	}
}
