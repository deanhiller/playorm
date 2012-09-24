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

	private List<ViewInfo> views;
	private DirectCursor<IndexColumnInfo> cursor;
	private NoSqlTypedSessionImpl session;
	private int batchSize;
	private String query;
	private DirectCursor<List<TypedRow>> cachedCursors = new EmptyCursor<List<TypedRow>>() ;

	public CursorAllViews(NoSqlTypedSessionImpl session, SpiMetaQuery metaQuery, DirectCursor<IndexColumnInfo> directCursor, int batchSize) {
		this.session = session;
		this.views = metaQuery.getTargetViews();
		this.query = metaQuery.getQuery();
		this.cursor = directCursor;
		this.batchSize = batchSize;
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
		List<DirectCursor<KeyValue<TypedRow>>> cursors = new ArrayList<DirectCursor<KeyValue<TypedRow>>>();
		createCursors(map, cursors);		
	}

	private void createCursors(Map<ViewInfo, List<byte[]>> map,
			List<DirectCursor<KeyValue<TypedRow>>> cursors) {
		for(ViewInfo view : views) {
			List<byte[]> rowKeys = map.get(view);
			DboTableMeta meta = view.getTableMeta();
			DirectCursor<KeyValue<TypedRow>> cursor = session.findAllImpl2(meta, null, rowKeys, query, batchSize);
			cursors.add(cursor);
		}
		
		cachedCursors = new Proxy(cursors, views);
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
			for(ViewInfo info : views) {
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
		for(ViewInfo view : views) {
			map.put(view, new ArrayList<byte[]>());
		}
	}
	
	private static class Proxy extends AbstractCursor<List<TypedRow>> {
		private List<DirectCursor<KeyValue<TypedRow>>> cursors;
		private List<ViewInfo> views;
		
		public Proxy(List<DirectCursor<KeyValue<TypedRow>>> cursors2, List<ViewInfo> views2) {
			this.cursors = cursors2;
			this.views = views2;
		}
		
		@Override
		public void beforeFirst() {
			for(DirectCursor<KeyValue<TypedRow>> d : cursors) {
				d.beforeFirst();
			}
		}

		@Override
		public Holder<List<TypedRow>> nextImpl() {
			
			boolean atLeastOneCursorHasNext = false;
			List<TypedRow> rows = new ArrayList<TypedRow>();
			for(int i = 0; i < cursors.size(); i++) {
				DirectCursor<KeyValue<TypedRow>> cursor = cursors.get(i);
				ViewInfo view = views.get(i);
				Holder<KeyValue<TypedRow>> next = cursor.nextImpl();
				if(next != null) {
					atLeastOneCursorHasNext = true;
					KeyValue<TypedRow> kv = next.getValue();
					TypedRow row = kv.getValue();
					if(row != null)
						row.setView(view);
					rows.add(row);
				} else
					rows.add(null);
			}

			if(atLeastOneCursorHasNext)
				return new Holder<List<TypedRow>>(rows);
			return null;
		}
	}
}
