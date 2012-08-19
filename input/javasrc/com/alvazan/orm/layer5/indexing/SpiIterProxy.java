package com.alvazan.orm.layer5.indexing;

import java.util.Iterator;

import com.alvazan.orm.api.spi9.db.Column;

public class SpiIterProxy implements Iterable<byte[]> {

	private Iterable<Column> scan;
	private int firstResult;
	private Integer maxResults;

	public SpiIterProxy(Iterable<Column> scan, int firstResult,
			Integer maxResults) {
		this.scan = scan;
		this.firstResult = firstResult;
		this.maxResults = maxResults;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new SpiIteratorProxy(scan.iterator(), firstResult, maxResults);
	}
	
	private static class SpiIteratorProxy implements Iterator<byte[]> {

		private Iterator<Column> iterator;
		private int firstResult;
		private Integer maxResults;
		private int counter = 0;
		
		public SpiIteratorProxy(Iterator<Column> iterator, int firstResult,
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
		public byte[] next() {
			if(maxResults != null) {
				if(counter >= firstResult+maxResults)
					throw new IllegalStateException("you are past maxResults now");
			}
			
			Column c = iterator.next();
			byte[] primaryKey = c.getName();
			return primaryKey;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and never will be");
		}
	}
}
