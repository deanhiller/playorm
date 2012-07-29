package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SetProxyFetchAll<T> extends OurAbstractCollection<T> implements CacheLoadCallback, Set<T> {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(SetProxyFetchAll.class);
	
	private static final long serialVersionUID = 1L;
	private NoSqlSession session;
	private MetaClass<T> classMeta;
	private boolean cacheLoaded = false;
	private Set<Holder<T>> holders = new HashSet<Holder<T>>();
	//This is only held in memory until we fill in from cache when/if needed
	private List<byte[]> originalKeys;
	private List<Holder<T>> originalHolders = new ArrayList<Holder<T>>();
	
	public SetProxyFetchAll(NoSqlSession session, MetaClass<T> classMeta, List<byte[]> keys) {
		this.session = session;
		this.classMeta = classMeta;
		this.originalKeys = keys;
		for(byte[] key : keys) {
			Holder h = new Holder(classMeta, session, key, this);
			holders.add(h);
			originalHolders.add(h);
		}
	}

	public Object clone() throws CloneNotSupportedException {
        SetProxyFetchAll v = (SetProxyFetchAll) super.clone();
        SetProxyFetchAll[] current = this.toArray(new SetProxyFetchAll[0]);
        SetProxyFetchAll[] clone = Arrays.copyOf(current, this.size());
        List<SetProxyFetchAll> asList = Arrays.asList(clone);
        v.addAll(asList);

        return v;
    }

	//Callback from one of the proxies to load the entire cache based
	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
	public void loadCacheIfNeeded() {
		if(cacheLoaded)
			return;
		
		List<Row> rows = session.find(classMeta.getColumnFamily(), originalKeys);
		for(int i = 0; i < this.size(); i++) {
			Row row = rows.get(i);
			Holder<T> h = (Holder) originalHolders.get(i);
			T value = h.getValue();
			if(value instanceof NoSqlProxy) {
				//inject the row into the proxy object here to load it's fields
				classMeta.fillInInstance(row, session, value);
				//((NoSqlProxy)value).__injectData(row);
			}
		}
		cacheLoaded = true;
	}
	
	@SuppressWarnings("hiding")
	@Override
	public <T> T[] toArray(T[] a) {
		Object[] elements = toArray();
		return (T[]) Arrays.copyOf(elements, this.size(), a.getClass());
	}

	@Override
	protected Collection<Holder<T>> getHolders() {
		return holders;
	}

}
