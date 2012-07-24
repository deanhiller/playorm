package com.alvazan.orm.layer2.nosql.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.alvazan.orm.api.spi.db.Action;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.db.NoSqlRawSession;
import com.alvazan.orm.api.spi.db.Persist;
import com.alvazan.orm.api.spi.db.Remove;
import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.index.IndexAdd;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.IndexRemove;
import com.alvazan.orm.api.spi.index.IndexRemoveImpl;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;

public class NoSqlWriteCacheImpl implements NoSqlSession {

	@Inject
	private NoSqlRawSession rawSession;
	@Inject
	private IndexReaderWriter indexWriter;
	
	private Map<String, List<? extends IndexRemove>> removeFromIndex = new HashMap<String, List<? extends IndexRemove>>(); 
	private List<Action> actions = new ArrayList<Action>();
	private Map<String, List<IndexAdd>> addToIndex = new HashMap<String, List<IndexAdd>>();
	
	@Override
	public void persist(String colFamily, byte[] rowKey, List<Column> columns) {
		Persist persist = new Persist();
		persist.setColFamily(colFamily);
		persist.setRowKey(rowKey);
		persist.setColumns(columns);
		actions.add(persist);
	}

	@Override
	public void remove(String colFamily, byte[] rowKey) {
		Remove remove = new Remove();
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		actions.add(remove);			
	}
	
	@Override
	public void remove(String colFamily, byte[] rowKey, List<byte[]> columnNames) {
		Remove remove = new Remove();
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		remove.setColumns(columnNames);
		actions.add(remove);		
	}

	@Override
	public List<Row> find(String colFamily, List<byte[]> keys) {
		return rawSession.find(colFamily, keys);
	}

	@Override
	public void flush() {
		//Cassandra uses timestamp for conflict resolution so don't provide that
//		long time = System.currentTimeMillis();
//		
//		for(Action action : actions) {
//			insertTime(action, time);
//		}

		//REMOVE from index BEFORE removing the entity.  ie. If we do the reverse, you do a query and get
		//an id of entity that is about to be removed and if removed before you, you don't get the entity
		indexWriter.sendRemoves(removeFromIndex);
		rawSession.sendChanges(actions);
		indexWriter.sendAdds(addToIndex);
	}

//	private void insertTime(Action action, long time) {
//		if(action instanceof Persist) {
//			((Persist)action).setTimestamp(time);
//		}
//	}

	@Override
	public NoSqlRawSession getRawSession() {
		return rawSession;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void removeFromIndex(String indexName, String id) {
		if(id == null)
			throw new IllegalArgumentException("id cannot be null");
		else if(indexName == null)
			throw new IllegalArgumentException("indexName cannot be null");
		IndexRemoveImpl remove = new IndexRemoveImpl();
		remove.setId(id);
		List removeActions = findCreateList(indexName, removeFromIndex);
		removeActions.add(remove);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void addToIndex(String indexName, String id, Map<String, Object> item) {
		if(id == null)
			throw new IllegalArgumentException("id cannot be null");
		else if(indexName == null)
			throw new IllegalArgumentException("indexName cannot be null");
		else if(item == null)
			throw new IllegalArgumentException("item map cannot be null");
		
		IndexAdd add = new IndexAdd();
		add.setItem(item);
		add.setId(id);
		
		List removeActions = findCreateList(indexName, removeFromIndex);
		List addActions = findCreateList(indexName, addToIndex);
		//ironically, we have to remove it form the index as well as the old data in the index has
		//most likely changed...
		removeActions.add(add);
		addActions.add(add);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List findCreateList(String indexName, Map indexToList) {
		List actions = (List) indexToList.get(indexName);
		if(actions == null) {
			actions = new ArrayList();
			indexToList.put(indexName, actions);
		}
		return actions;
	}
	
	@Override
	public IndexReaderWriter getRawIndex() {
		return indexWriter;
	}

	@Override
	public void clearDbAndIndexesIfInMemoryType() {
		rawSession.clearDatabaseIfInMemoryType();
		indexWriter.clearIndexesIfInMemoryType();
		
	}

	@Override
	public Iterable<Column> columnRangeScan(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) {
		return rawSession.columnRangeScan(colFamily, rowKey, from, to, batchSize);
	}

}
