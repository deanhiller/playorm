package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.base.Cursor;
import com.alvazan.orm.api.z5api.IndexColumnInfo;

public class IterableIndex implements Iterable<byte[]> {

	private Cursor<IndexColumnInfo> cursor;
	public IterableIndex(Cursor<IndexColumnInfo> indice) {
		this.cursor = indice;
	}

	@Override
	public Iterator<byte[]> iterator() {
		cursor.beforeFirst();
		return new IndexIterator(cursor);
	}
	
	private static class IndexIterator implements Iterator<byte[]> {

		private Cursor<IndexColumnInfo> cursor;

		public IndexIterator(Cursor<IndexColumnInfo> cursor) {
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			return this.cursor.hasNext();
		}

		@Override
		public byte[] next() {
			return cursor.next().getPrimary().getPrimaryKey();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("This operation is not supported");
		}
	}
}
