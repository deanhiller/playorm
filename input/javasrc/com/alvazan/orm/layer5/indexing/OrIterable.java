package com.alvazan.orm.layer5.indexing;

import java.util.Iterator;

import com.alvazan.orm.api.spi3.meta.IndexColumnInfo;

public class OrIterable implements Iterable<IndexColumnInfo> {

	private Iterable<IndexColumnInfo> leftResults;
	private Iterable<IndexColumnInfo> rightResults;

	public OrIterable(Iterable<IndexColumnInfo> leftResults,
			Iterable<IndexColumnInfo> rightResults) {
		this.leftResults = leftResults;
		this.rightResults = rightResults;
	}

	@Override
	public Iterator<IndexColumnInfo> iterator() {
		return new OrIterator(leftResults.iterator(), rightResults.iterator());
	}
	
	private static class OrIterator implements Iterator<IndexColumnInfo> {

		private Iterator<IndexColumnInfo> leftResults;
		private Iterator<IndexColumnInfo> rightResults;

		public OrIterator(Iterator<IndexColumnInfo> leftResults,
				Iterator<IndexColumnInfo> rightResults) {
			this.leftResults = leftResults;
			this.rightResults = rightResults;
		}

		@Override
		public boolean hasNext() {
			if(leftResults.hasNext() || rightResults.hasNext())
				return true;
			return false;
		}

		@Override
		public IndexColumnInfo next() {
			if(leftResults.hasNext())
				return leftResults.next();
			return rightResults.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}
}
