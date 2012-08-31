package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.z5api.IndexColumnInfo;

public class IterableIndex implements Iterable<byte[]> {

	private Iterable<IndexColumnInfo> iterable;

	public IterableIndex(Iterable<IndexColumnInfo> iter) {
		this.iterable = iter;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IndexIterator(iterable.iterator());
	}
	
	private static class IndexIterator implements Iterator<byte[]> {

		private Iterator<IndexColumnInfo> iterator;

		public IndexIterator(Iterator<IndexColumnInfo> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return this.iterator.hasNext();
		}

		@Override
		public byte[] next() {
			return iterator.next().getPrimary().getPrimaryKey();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("This operation is not supported");
		}
	}
}
