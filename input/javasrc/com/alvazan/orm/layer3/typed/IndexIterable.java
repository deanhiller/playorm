package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.spi9.db.IndexColumn;

public class IndexIterable implements Iterable<byte[]> {

	private Iterable<IndexColumn> iterable;

	public IndexIterable(Iterable<IndexColumn> iter) {
		this.iterable = iter;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IndexIterator(iterable.iterator());
	}
	
	private static class IndexIterator implements Iterator<byte[]> {

		private Iterator<IndexColumn> iterator;

		public IndexIterator(Iterator<IndexColumn> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return this.iterator.hasNext();
		}

		@Override
		public byte[] next() {
			return iterator.next().getPrimaryKey();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("This operation is not supported");
		}
	}
}
