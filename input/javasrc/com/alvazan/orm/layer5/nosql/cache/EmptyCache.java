package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;

public class EmptyCache implements Cache {

	private Cache realCache;
	
	public EmptyCache(Cache c) {
		if(c == null)
			throw new IllegalArgumentException("c can't be null");
		realCache = c;
	}
	
	@Override
	public RowHolder<Row> fromCache(String colFamily, byte[] key) {
		return null;
	}

	//For queries we want to cache every row still....we just get all values from database
	@Override
	public void cacheRow(String colFamily, byte[] b, Row value) {
		realCache.cacheRow(colFamily, b, value);
	}
}
