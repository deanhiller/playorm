package com.alvazan.orm.layer5.indexing;

import java.util.Iterator;

import com.alvazan.orm.api.spi9.db.IndexColumn;

public class SpiIterProxy implements Iterable<IndexColumn> {

	private Iterable<IndexColumn> scan;
	private int firstResult;
	private Integer maxResults;

	public SpiIterProxy(Iterable<IndexColumn> scan, int firstResult,
			Integer maxResults) {
		this.scan = scan;
		this.firstResult = firstResult;
		this.maxResults = maxResults;
	}

	@Override
	public Iterator<IndexColumn> iterator() {
		return new SpiIteratorProxy(scan.iterator(), firstResult, maxResults);
	}
	
	private static class SpiIteratorProxy implements Iterator<IndexColumn> {

		private Iterator<IndexColumn> iterator;
		private int firstResult;
		private Integer maxResults;
		private int counter = 0;
		
		public SpiIteratorProxy(Iterator<IndexColumn> iterator, int firstResult,
				Integer maxResults) {
			this.iterator = iterator;
			this.firstResult = firstResult;
			this.maxResults = maxResults;
		}

		@Override
		public boolean hasNext() {
			while(counter < firstResult && iterator.hasNext()) {
				iterator.next(); //must skip entries until we are at first result
				counter++;
			}
			if(maxResults != null) {
				if(counter >= firstResult+maxResults)
					return false;
			}
			return iterator.hasNext();
		}

		@Override
		public IndexColumn next() {
			if(maxResults != null) {
				if(counter >= firstResult+maxResults)
					throw new IllegalStateException("you are past maxResults now");
			}
			
			return iterator.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and never will be");
		}
	}
}
