package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.z8spi.conv.Converter;

public class SimpleMap<K, V> implements Map<K, V>{

	private boolean removeAll;
	protected Map<K, V> added = new HashMap<K, V>();
	private Map<K, V> originalValues = new HashMap<K, V>();
	protected HashMap<K, V> values = new HashMap<K, V>();
	
	public SimpleMap(Converter converter, Converter valueConverter,
			Map<byte[], byte[]> theCols) {
		for(Entry<byte[], byte[]> entry : theCols.entrySet()) {
			K key = (K) converter.convertFromNoSql(entry.getKey());
			V val = (V) valueConverter.convertFromNoSql(entry.getValue());
    		this.values.put(key, val);
    		originalValues.put(key, val);		
		}
	}
	@Override
	public int size() {
		return values.size();
	}
	@Override
	public boolean isEmpty() {
		return values.isEmpty();
	}
	@Override
	public boolean containsKey(Object key) {
		return values.containsKey(key);
	}
	@Override
	public boolean containsValue(Object value) {
		return values.containsValue(value);
	}
	@Override
	public V get(Object key) {
		return values.get(key);
	}
	@Override
	public V put(K key, V value) {
		added.put(key, value);
		return values.put(key, value);
	}
	@Override
	public V remove(Object key) {
		added.remove(key);
		return values.remove(key);
	}
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		added.putAll(m);
		values.putAll(m);
	}
	@Override
	public void clear() {
		removeAll = true;
		added.clear();
		values.clear();		
	}

	@Override
	public Set<K> keySet() {
		return values.keySet();
	}
	@Override
	public Collection<V> values() {
		return values.values();
	}
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return values.entrySet();
	}
	
	public Collection<K> getToBeRemoved() {
		List<K> removes = new ArrayList<K>();
		for(K k : originalValues.keySet()) {
			if(values.get(k) == null)
				removes.add(k);
		}
		return removes;
	}
	
	public Map<K, V> getToBeAdded() {
		return added;
	}
}
