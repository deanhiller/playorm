package com.alvazan.orm.layer5.indexing;

import java.util.Iterator;

import com.alvazan.orm.api.spi3.meta.IndexColumnInfo;
import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.util.CachingIterator;

public class IterableForAnd implements Iterable<IndexColumnInfo> {

	private Iterable<IndexColumnInfo> leftResults;
	private Iterable<IndexColumnInfo> rightResults;

	public IterableForAnd(Iterable<IndexColumnInfo> leftResults,
			Iterable<IndexColumnInfo> rightResults) {
		this.leftResults = leftResults;
		this.rightResults = rightResults;
	}

	@Override
	public Iterator<IndexColumnInfo> iterator() {
		return new AndIterator(leftResults.iterator(), rightResults);
	}
	
	private static class AndIterator extends CachingIterator<IndexColumnInfo> {

		private Iterator<IndexColumnInfo> leftResults;
		private Iterable<IndexColumnInfo> rightResults;
		private IndexColumnInfo lastCachedResult;
		
		public AndIterator(Iterator<IndexColumnInfo> leftResults,
				Iterable<IndexColumnInfo> rightResults2) {
			this.leftResults = leftResults;
			this.rightResults = rightResults2;
		}

		@Override
		public boolean hasNextImpl() {
			while(leftResults.hasNext()) {
				IndexColumnInfo next = leftResults.next();
				//This stinks as we have to re-read from the database every 1000 rows!!!! We should find out if
				//we can do any kind of counting or something so we don't have to go back to database every time
				Iterator<IndexColumnInfo> rightIterator = rightResults.iterator();
				while(rightIterator.hasNext()) {
					IndexColumnInfo andedInfo = rightIterator.next();
					ByteArray key1 = next.getPrimaryKey();
					ByteArray key2 = andedInfo.getPrimaryKey();
					if(key1.equals(key2)) {
						next.setNextAndedColumn(andedInfo);
						lastCachedResult = next;
						return true;
					}
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
