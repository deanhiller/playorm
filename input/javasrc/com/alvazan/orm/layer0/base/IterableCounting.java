package com.alvazan.orm.layer0.base;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractIterator;

public class IterableCounting implements Iterable<byte[]> {

	private AbstractIterator<byte[]> keysIterator;
	private boolean alreadyRun = false;
	private int batchSize;
	public IterableCounting(AbstractIterator<byte[]> keysIterator, int batchSize2) {
		Precondition.check(keysIterator, "keysIterator");
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

	private static class CountingIterator extends AbstractIterator<byte[]>{
		private AbstractIterator<byte[]> keysIterator;
		private int count = 0;
		private int batchSize;
		public CountingIterator(AbstractIterator<byte[]> keysIterator, int batchSize2) {
			this.keysIterator = keysIterator;
			this.batchSize = batchSize2;
		}

		@Override
		public com.alvazan.orm.api.z8spi.iter.AbstractIterator.IterHolder<byte[]> nextImpl() {
			if(count >= batchSize)
				return null;
			count++;
			return keysIterator.nextImpl();
		}
	}
}
