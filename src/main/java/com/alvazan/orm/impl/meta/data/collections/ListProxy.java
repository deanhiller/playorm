package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ListProxy<T> extends ArrayList<T> {

	private static final long serialVersionUID = 1L;
	private MetaAbstractClass<T> metaClass;

	public ListProxy(NoSqlSession session, MetaAbstractClass<T> classMeta, List<byte[]> keys) {
		this.metaClass = classMeta;
		for(byte[] key : keys) {
			Holder h = new Holder(classMeta, session, key, null);
			super.add((T) h);
		}
	}

	public Object clone() {
        ListProxy v = (ListProxy) super.clone();
        ListProxy[] current = this.toArray(new ListProxy[0]);
        ListProxy[] clone = Arrays.copyOf(current, this.size());
        List<ListProxy> asList = Arrays.asList(clone);
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

	private T getImpl(int i) {
		//CHECK if already converted....
		Holder h = (Holder) super.get(i);
		return (T) h.getValue();
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
		Holder<T> h = new Holder<T>(metaClass, element);
		return super.set(index, (T) h);
	}

	@Override
	public boolean add(T e) {
		Holder<T> h = new Holder<T>(metaClass, e);
		return super.add((T) h);
	}

	@Override
	public void add(int index, T element) {
		Holder<T> h = new Holder<T>(metaClass, element);
		super.add(index, (T) h);
	}

	@Override
	public T remove(int index) {
		return super.remove(index);
	}

	@Override
	public boolean remove(Object o) {
		Holder h = new Holder(metaClass, o);
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
			Holder h = new Holder(metaClass, val);
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
		Collection holders = createHolders(c);
		return super.removeAll(holders);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
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
			Holder h = new Holder(metaClass, e);
			delegate.set(h);
		}

		@Override
		public void add(T e) {
			Holder h = new Holder(metaClass, e);
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
