package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;

import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ListProxyFetchAll<T> extends ArrayList<T> implements CacheLoadCallback {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(ListProxyFetchAll.class);
	
	private static final long serialVersionUID = 1L;
	private NoSqlSession session;
	private MetaClass<T> classMeta;
	private List<byte[]> keys;
	private boolean cacheLoaded = false;
	
	public ListProxyFetchAll(NoSqlSession session, MetaClass<T> classMeta, List<byte[]> keys) {
		this.session = session;
		this.classMeta = classMeta;
		this.keys = keys;
		for(byte[] key : keys) {
			Holder h = new Holder(classMeta, session, key, this);
			super.add((T) h);
			String temp = new String(key);
			log.info("temp="+temp);
		}
	}

	public Object clone() {
        ListProxyFetchAll v = (ListProxyFetchAll) super.clone();
        ListProxyFetchAll[] current = this.toArray(new ListProxyFetchAll[0]);
        ListProxyFetchAll[] clone = Arrays.copyOf(current, this.size());
        List<ListProxyFetchAll> asList = Arrays.asList(clone);
        v.addAll(asList);

        return v;
    }

	@Override
	public Object[] toArray() {
		Object[] elements = new Object[this.size()];
		for(int i = 0; i < this.size(); i++) {
			elements[i] = this.getImpl(i);
		}
		
		return elements;
	}

	private T getImpl(int index) {
		Holder<T> holder = (Holder) super.get(index);
		return holder.getValue();
	}
	
	//Callback from one of the proxies to load the entire cache based
	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
	public void loadCacheIfNeeded() {
		if(cacheLoaded)
			return;
		
		List<Row> rows = session.find(classMeta.getColumnFamily(), keys);
		for(int i = 0; i < this.size(); i++) {
			Row row = rows.get(i);
			Holder<T> h = (Holder) super.get(i);
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
	public T get(int index) {
		return getImpl(index);
	}

	@Override
	public T set(int index, T element) {
		Holder<T> h = new Holder<T>(element);
		return super.set(index, (T) h);
	}

	@Override
	public boolean add(T e) {
		Holder<T> h = new Holder<T>(e);
		return super.add((T) h);
	}

	@Override
	public void add(int index, T element) {
		Holder<T> h = new Holder<T>(element);
		super.add(index, (T) h);
	}

	@Override
	public T remove(int index) {
		return super.remove(index);
	}

	@Override
	public boolean remove(Object o) {
		loadCacheIfNeeded(); //we have to do this so equals will work
		Holder h = new Holder(o);
		return super.remove(h);
	}

	@Override
	public void clear() {
		super.clear();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Collection holders = createHolders(c);
		return super.addAll(holders);
	}

	private Collection createHolders(Collection c) {
		Collection holders = new ArrayList();
		for(Object val : c) {
			Holder h = new Holder(val);
			holders.add(h);
		}
		return holders;
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		Collection holders = createHolders(c);
		return super.addAll(index, holders);
	}

	@Override
	protected void removeRange(int fromIndex, int toIndex) {
		super.removeRange(fromIndex, toIndex);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		loadCacheIfNeeded();
		Collection holders = createHolders(c);
		return super.removeAll(holders);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		loadCacheIfNeeded();
		Collection holders = createHolders(c);
		return super.retainAll(holders);
	}

	private class OurIter implements ListIterator<T> {

		private ListIterator delegate;

		public OurIter(ListIterator delegate) {
			this.delegate = delegate;
		}
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public T next() {
			Holder<T> holder = (Holder<T>) delegate.next();
			return holder.getValue();
		}

		@Override
		public boolean hasPrevious() {
			return delegate.hasPrevious();
		}

		@Override
		public T previous() {
			Holder<T> holder = (Holder<T>) delegate.previous();
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
		ListIterator<T> iter = super.listIterator(index);
		OurIter proxy = new OurIter(iter);
		return proxy;
	}

	@Override
	public ListIterator<T> listIterator() {
		return listIterator(0);
	}

	private class OurItr implements Iterator<T> {
		private Iterator delegate;

		public OurItr(Iterator iter) {
			delegate = iter;
		}
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public T next() {
			Holder holder = (Holder) delegate.next();
			return (T) holder.getValue();
		}

		@Override
		public void remove() {
			delegate.remove();
		}
		
	}
	
	@Override
	public Iterator<T> iterator() {
		return new OurItr(super.iterator());
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException("not supported yet");
	}
}
