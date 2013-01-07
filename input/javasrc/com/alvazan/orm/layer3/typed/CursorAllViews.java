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
import com.alvazan.orm.api.z8spi.iter.IterableWrappingCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
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
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorAllViews(mergingcursor)["+tabs+cursor+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	@Override
	public void beforeFirst() {
		cachedCursors = new EmptyCursor<List<TypedRow>>();
		cursor.beforeFirst();
	}
	
	@Override
	public void afterLast() {
		cachedCursors = new EmptyCursor<List<TypedRow>>();
		cursor.afterLast();
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
	
	@Override
	public Holder<List<TypedRow>> previousImpl() {
		Holder<List<TypedRow>> previous = cachedCursors.previousImpl();
		if(previous != null)
			return previous;

		//Well, the cursor ran out, load more results if any exist...
		loadCacheBackward();
		
		return cachedCursors.previousImpl();
	}

	private void loadCache() {
		Map<ViewInfo, TwoLists> map = setupKeyLists();
		createCursors(map);		
	}
	
	private void loadCacheBackward() {
		Map<ViewInfo, TwoLists> map = setupKeyListsBackward();
		createCursors(map);		
	}

	private void createCursors(Map<ViewInfo, TwoLists> map) {
		List<DirectCursor<KeyValue<TypedRow>>> cursors = new ArrayList<DirectCursor<KeyValue<TypedRow>>>();
		List<List<byte[]>> fullKeyLists = new ArrayList<List<byte[]>>();
		
		for(ViewInfo view : eagerlyJoinedViews) {
			TwoLists twoLists = map.get(view);
			List<byte[]> rowKeys = twoLists.getListWithNoNulls();
			DboTableMeta meta = view.getTableMeta();
			DirectCursor<KeyValue<TypedRow>> cursor = session.findAllImpl2(meta, null, new IterableWrappingCursor<byte[]>(rowKeys), query, batchSize);
			DirectCursor<KeyValue<TypedRow>> fillCursor = new CursorFillNulls(cursor, twoLists.getFullKeyList(), view);
			cursors.add(fillCursor);
			fullKeyLists.add(twoLists.getFullKeyList());
		}
		
		cachedCursors = new CursorJoinedViews(cursors, eagerlyJoinedViews);
		
//		if(delayedJoinViews.size() > 0) {
//			cachedCursors = new CursorDelayedJoins(cachedCursors, eagerlyJoinedViews);
//		}
	}

	private Map<ViewInfo, TwoLists> setupKeyLists() {
		Map<ViewInfo, TwoLists> map = new HashMap<ViewInfo, TwoLists>();
		initializeMap(map);
		
		for(int i = 0; i < batchSize; i++) {
			//Here we want to read in batchSize
			Holder<IndexColumnInfo> next = cursor.nextImpl();
			if(next == null)
				break;
			
			
			IndexColumnInfo index = next.getValue();
			if(index != null) {
				for(ViewInfo info : eagerlyJoinedViews) {
					byte[] pk = index.getPrimaryKeyRaw(info);
					TwoLists twoLists = map.get(info);
					twoLists.getFullKeyList().add(pk);
					if(pk != null) {
						List<byte[]> list = twoLists.getListWithNoNulls();
						list.add(pk);
					}
				}
			}
		}
		return map;
	}
	
	private Map<ViewInfo, TwoLists> setupKeyListsBackward() {
		Map<ViewInfo, TwoLists> map = new HashMap<ViewInfo, TwoLists>();
		initializeMap(map);
		
		for(int i = 0; i < batchSize; i++) {
			//Here we want to read in batchSize
			Holder<IndexColumnInfo> previous = cursor.previousImpl();
			if(previous == null)
				break;
			
			
			IndexColumnInfo index = previous.getValue();
			for(ViewInfo info : eagerlyJoinedViews) {
				byte[] pk = index.getPrimaryKeyRaw(info);
				TwoLists twoLists = map.get(info);
				twoLists.getFullKeyList().add(pk);
				if(pk != null) {
					List<byte[]> list = twoLists.getListWithNoNulls();
					list.add(pk);
				}
			}
		}
		return map;
	}

	private void initializeMap(Map<ViewInfo, TwoLists> map) {
		for(ViewInfo view : eagerlyJoinedViews) {
			map.put(view, new TwoLists());
		}
	}
	
	private static class TwoLists {
		private List<byte[]> fullKeyList = new ArrayList<byte[]>();
		private List<byte[]> listWithNoNulls = new ArrayList<byte[]>();
		public List<byte[]> getFullKeyList() {
			return fullKeyList;
		}
		public List<byte[]> getListWithNoNulls() {
			return listWithNoNulls;
		}
	}
}
