package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.IterToVirtual;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.Tuple;

public class CursorProxy<T> implements CursorToMany<T> {

	private AbstractCursor<IndexColumn> cursor;
	private NoSqlSession session;
	private MetaAbstractClass<T> proxyMeta;
	private int batchSize;
	private Iterator<T> cachedProxies;
	private T currentProxy;
	private List<T> proxyList;
	private List<byte[]> keyList;
	private Object owner;
	private List<T> elementsToAdd = new ArrayList<T>();
	private List<T> elementsToRemove = new ArrayList<T>();
	private boolean currentCacheLoaded = false;
			
	public CursorProxy(Object owner, NoSqlSession session,
			AbstractCursor<IndexColumn> indexCursor,
			MetaAbstractClass<T> proxyMeta, int batchSize) {
		this.owner = owner;
		this.session = session;
		this.cursor = indexCursor;
		this.proxyMeta = proxyMeta;
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}

	@Override
	public boolean next() {
		loadSomeKeys();
		if(cachedProxies.hasNext()) {
			currentProxy = cachedProxies.next();
			return true;
		}
		currentProxy = null;
		return false;
	}

	private void loadSomeKeys() {
		if(cachedProxies != null && cachedProxies.hasNext())
			return;
		
		proxyList = new ArrayList<T>();
		keyList = new ArrayList<byte[]>();
		while(true) {
			Holder<IndexColumn> holder = cursor.nextImpl();
			if(holder == null)
				break;
			IndexColumn val = holder.getValue();
			//NOTE: Here the indCol.getPrimaryKey is our owning entities primary key
			// and the indexedValue is the actual foreign key to the other table
			byte[] indexedValue = val.getIndexedValue();
			keyList.add(indexedValue);
			T proxy = convertIdToProxy(indexedValue, session);
			proxyList.add(proxy);
			if(proxyList.size() > batchSize)
				break;
		}

		cachedProxies = proxyList.iterator();
		//new proxies that are not initialized...
		currentCacheLoaded = false;
	}

	@Override
	public T getCurrent() {
		if(currentProxy == null)
			throw new IllegalArgumentException("There is no element located at this position...check the boolean returned from next before calling getCurrent");
		
		return currentProxy;
	}
	
	public T convertIdToProxy(byte[] id, NoSqlSession session) {
		Tuple<T> tuple = proxyMeta.convertIdToProxy(null, null, id, new LoadCacheCallback());
		return tuple.getProxy();
	}

	private void loadCache() {
		if(this.currentCacheLoaded)
			return;
		
		currentCacheLoaded = true;
		
		DboTableMeta metaDbo = proxyMeta.getMetaDbo();
		Iterable<byte[]> virtKeys = new IterToVirtual(metaDbo, keyList);
		AbstractCursor<KeyValue<Row>> rows = session.find(metaDbo, virtKeys, true, batchSize);
		
		int counter = 0;
		while(true) {
			com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> holder = rows.nextImpl();
			if(holder == null)
				break;
			KeyValue<Row> kv = holder.getValue();
			byte[] key = (byte[]) kv.getKey();
			Row row = kv.getValue();
			Tuple<T> tuple = proxyMeta.convertIdToProxy(row, session, key, null);
			if(row == null) {
				throw new IllegalStateException("This entity is corrupt(your entity='"+owner+"') and contains a" +
						" reference/FK to a row that does not exist in another table.  " +
						"It refers to another entity with pk="+tuple.getEntityId()+" which does not exist");
			}
			T value = proxyList.get(counter);
			proxyMeta.fillInInstance(row, session, value);
			counter++;
		}
	}
	
	private class LoadCacheCallback implements CacheLoadCallback {
		@Override
		public void loadCacheIfNeeded() {
			loadCache();
		}
	}
	
	@Override
	public void removeCurrent() {
		elementsToRemove.add(getCurrent());
	}

	@Override
	public void addElement(T element) {
		elementsToAdd.add(element);
	}

	public List<T> getElementsToAdd() {
		return elementsToAdd;
	}

	public List<T> getElementsToRemove() {
		return elementsToRemove;
	}

}
