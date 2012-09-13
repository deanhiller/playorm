package com.alvazan.orm.layer3.typed;

import java.util.List;

import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class QueryResultImpl implements QueryResult {

	private SpiMetaQuery metaQuery;
	private NoSqlTypedSessionImpl session;
	private DirectCursor<IndexColumnInfo> directCursor;
	private int batchSize;

	public QueryResultImpl(SpiMetaQuery metaQuery2,
			NoSqlTypedSessionImpl noSqlTypedSessionImpl,
			DirectCursor<IndexColumnInfo> iter, int batchSize) {
		this.metaQuery = metaQuery2;
		this.session = noSqlTypedSessionImpl;
		this.directCursor = iter;
		this.batchSize = batchSize;
	}

	public Cursor<IndexColumnInfo> getCursor() {
		Cursor<IndexColumnInfo> cursor = new CursorProxyDirect(directCursor);
		cursor.beforeFirst();
		return cursor;
	}

	public List<ViewInfo> getViews() {
		return metaQuery.getAliases();
	}

	@Override
	public Cursor<List<TypedRow>> getAllViewsCursor() {
		
		while(true) {
			Holder<IndexColumnInfo> holder = directCursor.nextImpl();
			if(holder == null)
				break; //we are out of results
		}
		
		return null;
	}

	@Override
	public Cursor<KeyValue<TypedRow>> getPrimaryViewCursor() {
		ViewInfo mainView = metaQuery.getTargetViews().get(0);
		Iterable<byte[]> indexIterable = new IterableIndex(mainView, directCursor);

		DboTableMeta meta = mainView.getTableMeta();
		Cursor<KeyValue<TypedRow>> results = session.findAllImpl2(meta, null, indexIterable, metaQuery.getQuery(), batchSize);
		
		return results;
	}
	
}
