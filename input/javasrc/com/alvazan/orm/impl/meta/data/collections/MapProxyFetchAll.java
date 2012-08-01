package com.alvazan.orm.impl.meta.data.collections;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaClass.Tuple;

public class MapProxyFetchAll<K, V> extends HashMap<K, V> implements CacheLoadCallback {

	private static final long serialVersionUID = 1L;
	private boolean cacheLoaded;
	private NoSqlSession session;
	private MetaClass<V> classMeta;
	private Field fieldForKey;
	private List<byte[]> keys;
	private Set<V> originals = new HashSet<V>();
	private boolean removeAll;
	
	public MapProxyFetchAll(NoSqlSession session, MetaClass<V> classMeta,
			List<byte[]> keys, Field fieldForKey) {
		this.session = session;
		this.classMeta = classMeta;
		this.keys = keys;
		this.fieldForKey = fieldForKey;
	}

	//Callback from one of the proxies to load the entire cache based
	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
	@SuppressWarnings("unchecked")
	public void loadCacheIfNeeded() {
		if(cacheLoaded)
			return;

		List<Row> rows = session.find(classMeta.getColumnFamily(), keys);
		for(int i = 0; i < this.size(); i++) {
			Row row = rows.get(i);
			Tuple<V> tuple = classMeta.convertIdToProxy(row.getKey(), session, null);
			V proxy = tuple.getProxy();
			//inject the row into the proxy object here to load it's fields
			classMeta.fillInInstance(row, session, proxy);
			
			K key = (K) tuple.getEntityId();
			super.put(key,  proxy);
			originals.add(proxy);
		}
		cacheLoaded = true;
	}

	@Override
	public int size() {
		if(cacheLoaded)
			return super.size();
		return keys.size();
	}

	@Override
	public boolean isEmpty() {
		if(cacheLoaded)
			return super.isEmpty();
		return keys.isEmpty();
	}

	@Override
	public V get(Object key) {
		loadCacheIfNeeded();
		return super.get(key);
	}

	@Override
	public boolean containsKey(Object key) {
		loadCacheIfNeeded();
		return super.containsKey(key);
	}

	@Override
	public V put(K key, V value) {
		loadCacheIfNeeded();
		return super.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		loadCacheIfNeeded();
		super.putAll(m);
	}

	@Override
	public V remove(Object key) {
		loadCacheIfNeeded();
		return super.remove(key);
	}

	@Override
	public void clear() {
		//no need to load from cache in this case, just clear both key list in
		//case they haven't loaded cache yet so when it loads it is super fast
		//and clear the hashtable in case they loaded already
		removeAll = true;
		super.clear();
	}

	@Override
	public boolean containsValue(Object value) {
		loadCacheIfNeeded();
		return super.containsValue(value);
	}

	@Override
	public Object clone() {
		loadCacheIfNeeded();
		return super.clone();
	}

	@Override
	public Set<K> keySet() {
		//Well, we don't know the keys since we have not loaded from cache as the
		//key could be any field and we only have ids in memory before cache is loaded
		//id could be a field and we could optimize later for that.
		loadCacheIfNeeded();
		return super.keySet();
	}

	@Override
	public Collection<V> values() {
		loadCacheIfNeeded();
		return super.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		loadCacheIfNeeded();
		return super.entrySet();
	}

	public Collection<V> getToBeRemoved() {
		List<V> removes = new ArrayList<V>();
		if(!removeAll && !cacheLoaded)
			return removes;
		
		//If remove all(clear method called) we still need to check in case they added some
		//back, but if !removeAll and !cacheLoaded, we know none were removed.
		Collection<V> current = values();
		for(V entity : originals) {
			if(!current.contains(entity))
				removes.add(entity);
		}
		return removes;
	}

	public Collection<V> getToBeAdded() {
		List<V> adds = new ArrayList<V>();
		if(!cacheLoaded)
			return adds;
			
		for(V entity : values()) {
			if(!originals.contains(entity))
				adds.add(entity);
		}
		return adds;
	}
	
}
