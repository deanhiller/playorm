package com.alvazan.orm.layer3.typed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.EmptyCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorAllViews extends AbstractCursor<List<TypedRow>> {

	private List<ViewInfo> eagerlyJoinedViews;
	private List<ViewInfo> delayedJoinViews;
	
	private DirectCursor<IndexColumnInfo> cursor;
	private NoSqlTypedSessionImpl session;
	private int batchSize;
	private String query;
	private DirectCursor<List<TypedRow>> cachedCursors = new EmptyCursor<List<TypedRow>>() ;


	public CursorAllViews(NoSqlTypedSessionImpl session, SpiMetaQuery metaQuery, DirectCursor<IndexColumnInfo> directCursor, int batchSize) {
		this.session = session;
		this.query = metaQuery.getQuery();
		this.cursor = directCursor;
		this.batchSize = batchSize;
		this.delayedJoinViews = metaQuery.getViewsDelayedJoin();
		this.eagerlyJoinedViews = metaQuery.getViewsEagerJoin();
	}

	@Override
	public void beforeFirst() {
		cachedCursors = new EmptyCursor<List<TypedRow>>();
		cursor.beforeFirst();
	}

	@Override
	public Holder<List<TypedRow>> nextImpl() {
		Holder<List<TypedRow>> next = cachedCursors.nextImpl();
		if(next != null)
			return next;

		//Well, the cursor ran out, load more results if any exist...
		loadCache();
		
		return cachedCursors.nextImpl();
	}

	private void loadCache() {
		Map<ViewInfo, List<byte[]>> map = setupKeyLists();
		createCursors(map);		
	}

	private void createCursors(Map<ViewInfo, List<byte[]>> map) {
		List<DirectCursor<KeyValue<TypedRow>>> cursors = new ArrayList<DirectCursor<KeyValue<TypedRow>>>();
		
		for(ViewInfo view : eagerlyJoinedViews) {
			List<byte[]> rowKeys = map.get(view);
			DboTableMeta meta = view.getTableMeta();
			DirectCursor<KeyValue<TypedRow>> cursor = session.findAllImpl2(meta, null, rowKeys, query, batchSize);
			cursors.add(cursor);
		}
		
		cachedCursors = new CursorJoinedViews(cursors, eagerlyJoinedViews);
		
		if(delayedJoinViews.size() > 0) {
			//TODO: wire in the delayed join views
		}
	}

	private Map<ViewInfo, List<byte[]>> setupKeyLists() {
		Map<ViewInfo, List<byte[]>> map = new HashMap<ViewInfo, List<byte[]>>();
		initializeMap(map);
		
		for(int i = 0; i < batchSize; i++) {
			//Here we want to read in batchSize
			Holder<IndexColumnInfo> next = cursor.nextImpl();
			if(next == null)
				break;
			
			
			IndexColumnInfo index = next.getValue();
			for(ViewInfo info : eagerlyJoinedViews) {
				byte[] pk = index.getPrimaryKeyRaw(info);
				if(pk != null) {
					List<byte[]> list = map.get(info);
					list.add(pk);
				}
			}
		}
		return map;
	}

	private void initializeMap(Map<ViewInfo, List<byte[]>> map) {
		for(ViewInfo view : eagerlyJoinedViews) {
			map.put(view, new ArrayList<byte[]>());
		}
	}
}
