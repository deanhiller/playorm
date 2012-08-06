package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ListProxyFetchAll<T> extends OurAbstractCollection<T> implements List<T> {

	@SuppressWarnings("unused")
	private static final long serialVersionUID = 1L;
	private List<Holder<T>> currentList = new ArrayList<Holder<T>>();
	//immutable structures that hold the things cached that would need to be loaded

	public ListProxyFetchAll(NoSqlSession session, MetaAbstractClass<T> classMeta, List<byte[]> keys) {
		super(session, classMeta);
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

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		Collection<Holder<T>> holdersColl = createHolders(c);
		added.addAll(holdersColl);
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
		added.add(holder);
		Holder<T> existing = currentList.set(index, holder);
		return existing.getValue();
	}

	@Override
	public void add(int index, T element) {
		Holder<T> holder = new Holder<T>(element);
		added.add(holder);
		currentList.add(index, holder);
	}

	@Override
	public T remove(int index) {
		loadCacheIfNeeded(); //done because otherwise getRemovedElements will return and not tell the
		//framework the removed entities because if cache is not loaded, it returns an empty list.
		//We have to be careful because the one that was removed could be added back as well...ugh
		Holder<T> holder = currentList.remove(index);
		added.remove(holder);
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
		return currentList.indexOf(holder);
	}

	private class OurIter implements ListIterator<T> {

		private ListIterator<Holder<T>> delegate;
		private Holder<T> lastReturned;
		
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
			lastReturned = holder;
			return holder.getValue();
		}

		@Override
		public boolean hasPrevious() {
			return delegate.hasPrevious();
		}

		@Override
		public T previous() {
			Holder<T> holder = delegate.previous();
			lastReturned = holder;
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
			added.remove(lastReturned);
			delegate.remove();
		}

		@Override
		public void set(T e) {
			Holder h = new Holder(e);
			added.add(h);
			delegate.set(h);
		}

		@Override
		public void add(T e) {
			Holder h = new Holder(e);
			added.add(h);
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
