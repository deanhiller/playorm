package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ListProxyFetchAll<T> extends OurAbstractCollection<T> implements CacheLoadCallback, List<T> {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(ListProxyFetchAll.class);
	
	private static final long serialVersionUID = 1L;
	private NoSqlSession session;
	private MetaClass<T> classMeta;
	private List<Holder<T>> currentList = new ArrayList<Holder<T>>();
	//immutable structures that hold the things cached that would need to be loaded
	private List<byte[]> keys;
	private List<Holder<T>> originalHolders = new ArrayList<Holder<T>>();
	private boolean cacheLoaded = false;
	
	public ListProxyFetchAll(NoSqlSession session, MetaClass<T> classMeta, List<byte[]> keys) {
		this.session = session;
		this.classMeta = classMeta;
		this.keys = keys;
		for(byte[] key : keys) {
			Holder h = new Holder(classMeta, session, key, this);
			originalHolders.add(h);
			currentList.add(h);
		}
	}

	public Object clone() throws CloneNotSupportedException {
        ListProxyFetchAll v = (ListProxyFetchAll) super.clone();
        ListProxyFetchAll[] current = this.toArray(new ListProxyFetchAll[0]);
        ListProxyFetchAll[] clone = Arrays.copyOf(current, this.size());
        List<ListProxyFetchAll> asList = Arrays.asList(clone);
        v.addAll(asList);

        return v;
    }

	//Callback from one of the proxies to load the entire cache based
	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
	public void loadCacheIfNeeded() {
		if(cacheLoaded)
			return;
		
		List<Row> rows = session.find(classMeta.getColumnFamily(), keys);
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

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		Collection holdersColl = createHolders(c);
		return currentList.addAll(index, holdersColl);
	}

	@Override
	public T get(int index) {
		Holder<T> holder = currentList.get(index);
		return holder.getValue();
	}

	@Override
	public T set(int index, T element) {
		Holder<T> holder = new Holder<T>(element);
		Holder<T> existing = currentList.set(index, holder);
		return existing.getValue();
	}

	@Override
	public void add(int index, T element) {
		Holder<T> holder = new Holder<T>(element);
		currentList.add(index, holder);
	}

	@Override
	public T remove(int index) {
		Holder<T> holder = currentList.remove(index);
		return holder.getValue();
	}

	@Override
	public int indexOf(Object o) {
		Holder<T> holder = new Holder<T>((T) o);
		return currentList.indexOf(holder);
	}

	@Override
	public int lastIndexOf(Object o) {
		Holder<T> holder = new Holder<T>((T) o);
		return currentList.indexOf(o);
	}

	private class OurIter implements ListIterator<T> {

		private ListIterator<Holder<T>> delegate;

		public OurIter(ListIterator<Holder<T>> delegate) {
			this.delegate = delegate;
		}
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public T next() {
			Holder<T> holder = delegate.next();
			return holder.getValue();
		}

		@Override
		public boolean hasPrevious() {
			return delegate.hasPrevious();
		}

		@Override
		public T previous() {
			Holder<T> holder = delegate.previous();
			return holder.getValue();			
		}

		@Override
		public int nextIndex() {
			return delegate.nextIndex();
		}

		@Override
		public int previousIndex() {
			return delegate.nextIndex();
		}

		@Override
		public void remove() {
			delegate.remove();
		}

		@Override
		public void set(T e) {
			Holder h = new Holder(e);
			delegate.set(h);
		}

		@Override
		public void add(T e) {
			Holder h = new Holder(e);
			delegate.add(h);
		}
		
	}
	
	@Override
	public ListIterator<T> listIterator(int index) {
		ListIterator<Holder<T>> iter = currentList.listIterator(index);
		OurIter proxy = new OurIter(iter);
		return proxy;
	}

	@Override
	public ListIterator<T> listIterator() {
		return listIterator(0);
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException("not supported yet");
	}

	@Override
	protected Collection<Holder<T>> getHolders() {
		return currentList;
	}

}
