package com.alvazan.orm.layer3.typed;

import java.util.List;

import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
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
	private List<ViewInfo> viewsNotJoinedYet;

	public QueryResultImpl(SpiMetaQuery metaQuery2,
			NoSqlTypedSessionImpl noSqlTypedSessionImpl,
			DirectCursor<IndexColumnInfo> iter, int batchSize, List<ViewInfo> viewsNotJoinedYet) {
		this.metaQuery = metaQuery2;
		this.session = noSqlTypedSessionImpl;
		this.directCursor = iter;
		this.batchSize = batchSize;
		this.viewsNotJoinedYet = viewsNotJoinedYet;
	}

	public Cursor<IndexColumnInfo> getCursor() {
		directCursor.beforeFirst();
		Cursor<IndexColumnInfo> cursor = new CursorProxyDirect(directCursor);
		cursor.beforeFirst();
		return cursor;
	}

	public List<ViewInfo> getViews() {
		return metaQuery.getTargetViews();
	}

	@Override
	public Cursor<List<TypedRow>> getAllViewsCursor() {
		directCursor.beforeFirst();
		Cursor<List<TypedRow>> cursor = new CursorAllViews(session, metaQuery, directCursor, batchSize, viewsNotJoinedYet);
		return cursor;
	}

	@Override
	public Cursor<KeyValue<TypedRow>> getPrimaryViewCursor() {
		directCursor.beforeFirst();
		ViewInfo mainView = metaQuery.getTargetViews().get(0);
		Iterable<byte[]> indexIterable = new IterableCursorProxy(mainView, directCursor);

		DboTableMeta meta = mainView.getTableMeta();
		Cursor<KeyValue<TypedRow>> results = session.findAllImpl2(meta, null, indexIterable, metaQuery.getQuery(), batchSize);
		
		return results;
	}

	@Override
	public Iterable<KeyValue<TypedRow>> getPrimaryViewIter() {
		Cursor<KeyValue<TypedRow>> cursor = getPrimaryViewCursor();
		Iterable<KeyValue<TypedRow>> proxy = new IterableProxy<TypedRow>(cursor);
		return proxy;
	}
	
}
