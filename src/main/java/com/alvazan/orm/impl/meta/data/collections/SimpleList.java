package com.alvazan.orm.impl.meta.data.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.conv.Converter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleList<T> extends SimpleAbstractCollection<T> implements List<T> {

	@SuppressWarnings("unused")
	private static final long serialVersionUID = 1L;

	public SimpleList(Converter converter, List<byte[]> keys) {
		super(converter, keys);
	}

    public SimpleList(List<T> keys) {
        super(keys);
    }

	public Object clone() throws CloneNotSupportedException {
        SimpleList v = (SimpleList) super.clone();
        SimpleList[] current = this.toArray(new SimpleList[0]);
        SimpleList[] clone = Arrays.copyOf(current, this.size());
        List<SimpleList> asList = Arrays.asList(clone);
        v.addAll(asList);

        return v;
    }

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		return added.addAll(c);
	}

	@Override
	public T get(int index) {
		return values.get(index);
	}

	@Override
	public T set(int index, T element) {
		added.add(element);
		return values.set(index, element);
	}

	@Override
	public void add(int index, T element) {
		added.add(element);
		values.add(index, element);
	}

	@Override
	public T remove(int index) {
		T holder = values.remove(index);
		added.remove(holder);
		return holder;
	}

	@Override
	public int indexOf(Object o) {
		return values.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return values.indexOf(o);
	}

	private class OurIter implements ListIterator<T> {

		private ListIterator<T> delegate;
		private T lastReturned;
		
		public OurIter(ListIterator<T> delegate) {
			this.delegate = delegate;
		}
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public T next() {
			lastReturned = delegate.next();
			return lastReturned;
		}

		@Override
		public boolean hasPrevious() {
			return delegate.hasPrevious();
		}

		@Override
		public T previous() {
			lastReturned = delegate.previous();
			return lastReturned;			
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
			added.add(e);
			delegate.set(e);
		}

		@Override
		public void add(T e) {
			added.add(e);
			delegate.add(e);
		}
		
	}
	
	@Override
	public ListIterator<T> listIterator(int index) {
		ListIterator<T> iter = values.listIterator(index);
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

}
