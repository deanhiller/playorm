package com.alvazan.orm.layer5.nosql.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi9.db.Row;

public class IterCacheKeysProxy implements Iterable<byte[]> {

	private static final Logger log = LoggerFactory.getLogger(IterCacheKeysProxy.class);
	private NoSqlReadCacheImpl cache;
	private String colFamily;
	private Iterable<byte[]> rowKeys;
	private IteratorCacheKeysProxy iterator;
	private List<RowHolder<Row>> rowsFromCache = new ArrayList<RowHolder<Row>>();
	
	public IterCacheKeysProxy(NoSqlReadCacheImpl cache, String colFamily, Iterable<byte[]> rowKeys) {
		this.cache = cache;
		this.colFamily = colFamily;
		this.rowKeys = rowKeys;
	}

	@Override
	public Iterator<byte[]> iterator() {
		iterator = new IteratorCacheKeysProxy(cache, colFamily, rowKeys.iterator(), rowsFromCache);
		return iterator;
	}

	public List<RowHolder<Row>> getFoundInCache() {
		return rowsFromCache;
	}
	
	private static class IteratorCacheKeysProxy implements Iterator<byte[]> {

		private NoSqlReadCacheImpl cache;
		private String colFamily;
		private Iterator<byte[]> rowKeys;
		private byte[] lastCachedKey;
		private List<RowHolder<Row>> rowsFromCache;
		private int counter = 0;
		
		public IteratorCacheKeysProxy(NoSqlReadCacheImpl cache2,
				String colFamily, Iterator<byte[]> rowKeys, List<RowHolder<Row>> rows) {
			this.cache = cache2;
			this.colFamily = colFamily;
			this.rowKeys = rowKeys;
			this.rowsFromCache = rows;
		}

		@Override
		public boolean hasNext() {
			while(rowKeys.hasNext()) {
				lastCachedKey = rowKeys.next();
				//look it up in cache, if exists, record it
				RowHolder<Row> result = cache.fromCache(colFamily, lastCachedKey);
				//Let's make it so if they call iterator twice and loop over each iterator that
				//our single List operations are idempotent so the list will not change...
				if(counter >= rowsFromCache.size())
					rowsFromCache.add(result); //may be null and just save null to be filled in later
				else
					rowsFromCache.set(counter, result);
				counter++;
				
				if(result == null) { 
					return true; //we have a result NOT found in cache, return true for has next so the client can fetch it
				}
				
				log.info("cache hit(need to profile/optimize)");
			}
			return false;
		}

		@Override
		public byte[] next() {
			if(lastCachedKey != null)
				return lastCachedKey;
			else if(!hasNext()) {
				throw new IllegalStateException("There are no more entries in this iterator");
			}
			
			if(lastCachedKey == null)
				throw new RuntimeException("bug, if hasNext worked, we should have lastCachedKey");
			return lastCachedKey;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and probably never will be");
		}
	}

}
