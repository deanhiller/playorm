package com.alvazan.orm.layer5.indexing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.alvazan.orm.api.spi3.meta.IndexColumnInfo;
import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.util.CachingIterator;

public class IterableForOr implements Iterable<IndexColumnInfo> {

	private Iterable<IndexColumnInfo> leftResults;
	private Iterable<IndexColumnInfo> rightResults;

	public IterableForOr(Iterable<IndexColumnInfo> leftResults,
			Iterable<IndexColumnInfo> rightResults) {
		this.leftResults = leftResults;
		this.rightResults = rightResults;
	}

	@Override
	public Iterator<IndexColumnInfo> iterator() {
		return new OrIterator(leftResults.iterator(), rightResults.iterator());
	}
	
	private static class OrIterator extends CachingIterator<IndexColumnInfo> {

		private Iterator<IndexColumnInfo> leftResults;
		private Iterator<IndexColumnInfo> rightResults;
		private Map<ByteArray, IndexColumnInfo> pksToAlreadyFound = new HashMap<ByteArray, IndexColumnInfo>();
		private IndexColumnInfo lastCachedResult;
		
		public OrIterator(Iterator<IndexColumnInfo> leftResults,
				Iterator<IndexColumnInfo> rightResults) {
			this.leftResults = leftResults;
			this.rightResults = rightResults;
		}

		@Override
		public boolean hasNextImpl() {
			while(leftResults.hasNext()) {
				lastCachedResult = leftResults.next();
				pksToAlreadyFound.put(lastCachedResult.getPrimaryKey(), lastCachedResult);
				return true;
			}
			
			//NOW, as we go through the results on the right side, make sure we filter out 
			//duplicate primary keys by checking ones that we already returned.
			while(rightResults.hasNext()) {
				lastCachedResult = rightResults.next();
				IndexColumnInfo found = pksToAlreadyFound.get(lastCachedResult.getPrimaryKey());
				if(found != null) {
					found.setNextOrColumn(lastCachedResult);
				} else {
					return true;
				}
			}
			
			return false;
		}

		@Override
		public IndexColumnInfo nextImpl() {
			return lastCachedResult;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}
}
