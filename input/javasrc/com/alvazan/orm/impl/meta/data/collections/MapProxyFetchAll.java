package com.alvazan.orm.impl.meta.data.collections;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class MapProxyFetchAll<K, V> extends HashMap<K, V> implements CacheLoadCallback {

	private static final long serialVersionUID = 1L;
	private boolean cacheLoaded;
	private NoSqlSession session;
	private MetaClass<V> classMeta;
	private List<byte[]> keys;
	private Field fieldForKey;
	
	public MapProxyFetchAll(NoSqlSession session, MetaClass<V> classMeta,
			List<byte[]> keys, Field fieldForKey) {
		this.session = session;
		this.classMeta = classMeta;
		this.keys = keys;
		this.fieldForKey = fieldForKey;
	}

	//Callback from one of the proxies to load the entire cache based
	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
	public void loadCacheIfNeeded() {
		if(cacheLoaded)
			return;

		List<Row> rows = session.find(classMeta.getColumnFamily(), keys);
		for(int i = 0; i < this.size(); i++) {
			Row row = rows.get(i);
			V proxy = classMeta.convertIdToProxy(row.getKey(), session, null);
			//inject the row into the proxy object here to load it's fields
			classMeta.fillInInstance(row, session, proxy);
			
			K key = findFieldValue(proxy);
			super.put(key,  proxy);
		}
		cacheLoaded = true;
	}

	@SuppressWarnings("unchecked")
	private K findFieldValue(V proxy) {
		try {
			return (K) fieldForKey.get(proxy);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
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
		keys.clear();
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
	
}
