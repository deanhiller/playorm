package com.alvazan.orm.layer5.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;

/** 
 * After iterating through any node that only contains less than 500 results, we will not go back
 * to noSQL store when using the Caching Cursor
 * @author dhiller
 *
 * @param <T>
 */
public class CachingCursor<T> implements DirectCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> cursor;
	private List<IndexColumnInfo> cached = new ArrayList<IndexColumnInfo>();
	private Iterator<IndexColumnInfo> cachedIter;
	private boolean cacheEnabled = false;
	
	public CachingCursor(DirectCursor<IndexColumnInfo> cursor) {
		Precondition.check(cursor, "cursor");
		this.cursor = cursor;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		if(cacheEnabled) {
			return fetchFromCache();
		}
		Holder<IndexColumnInfo> next = cursor.nextImpl();
		if(next != null) {
			if(cached.size() < 500)
				cached.add(next.getValue());
			return next;
		}
		
		if(cached.size() < 500) {
			cacheEnabled = true;
		}
		return null;
	}

	private Holder<IndexColumnInfo> fetchFromCache() {
		if(cachedIter == null)
			return null;
		else if(!cachedIter.hasNext())
			return null;
		IndexColumnInfo next = cachedIter.next();
		return new Holder<IndexColumnInfo>(next.copy());
	}

	@Override
	public void beforeFirst() {
		if(cacheEnabled)
			cachedIter = cached.iterator();
		else
			cursor.beforeFirst();
	}
}
