package com.alvazan.orm.layer5.indexing;

import java.util.Iterator;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.IndexColumnInfo;
import com.alvazan.orm.api.spi9.db.IndexColumn;

public class SpiIterProxy implements Iterable<IndexColumnInfo> {

	private Iterable<IndexColumn> scan;
	private int firstResult;
	private Integer maxResults;
	private DboColumnMeta info;

	public SpiIterProxy(DboColumnMeta info, Iterable<IndexColumn> scan, int firstResult,
			Integer maxResults) {
		this.info = info;
		this.scan = scan;
		this.firstResult = firstResult;
		this.maxResults = maxResults;
	}

	@Override
	public Iterator<IndexColumnInfo> iterator() {
		return new SpiIteratorProxy(info, scan.iterator(), firstResult, maxResults);
	}
	
	private static class SpiIteratorProxy implements Iterator<IndexColumnInfo> {

		private Iterator<IndexColumn> iterator;
		private int firstResult;
		private Integer maxResults;
		private int counter = 0;
		private DboColumnMeta info;
		
		public SpiIteratorProxy(DboColumnMeta info, Iterator<IndexColumn> iterator, int firstResult,
				Integer maxResults) {
			this.iterator = iterator;
			this.firstResult = firstResult;
			this.maxResults = maxResults;
			this.info = info;
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
		public IndexColumnInfo next() {
			if(maxResults != null) {
				if(counter >= firstResult+maxResults)
					throw new IllegalStateException("you are past maxResults now");
			}
			
			IndexColumn indCol = iterator.next();
			IndexColumnInfo info = new IndexColumnInfo();
			info.setPrimary(indCol);
			info.setColumnMeta(this.info);
			return info;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and never will be");
		}
	}
}
