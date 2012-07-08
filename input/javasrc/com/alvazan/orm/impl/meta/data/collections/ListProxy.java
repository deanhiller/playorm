package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ListProxy<T> extends ArrayList<T> {

    @SuppressWarnings("rawtypes")
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
			Object primaryKey = this.get(i);
			elements[i] = wrapKeyInProxy(primaryKey);
		}
		
		return elements;
	}

	private Object wrapKeyInProxy(Object key) {
		throw new UnsupportedOperationException("not done yet");
	}
	
	@SuppressWarnings({ "unchecked", "hiding" })
	@Override
	public <T> T[] toArray(T[] a) {
		Object[] elements = toArray();
		return (T[]) Arrays.copyOf(elements, this.size(), a.getClass());
	}

	@Override
	public T get(int index) {
		throw new UnsupportedOperationException("not supported yet");
	}

	@Override
	public T set(int index, T element) {
		
		return super.set(index, element);
	}

	@Override
	public boolean add(T e) {
		
		return super.add(e);
	}

	@Override
	public void add(int index, T element) {
		
		super.add(index, element);
	}

	@Override
	public T remove(int index) {
		
		return super.remove(index);
	}

	@Override
	public boolean remove(Object o) {
		
		return super.remove(o);
	}

	@Override
	public void clear() {
		
		super.clear();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		
		return super.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		
		return super.addAll(index, c);
	}

	@Override
	protected void removeRange(int fromIndex, int toIndex) {
		
		super.removeRange(fromIndex, toIndex);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		
		return super.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		
		return super.retainAll(c);
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		
		return super.listIterator(index);
	}

	@Override
	public ListIterator<T> listIterator() {
		
		return super.listIterator();
	}

	@Override
	public Iterator<T> iterator() {
		
		return super.iterator();
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return super.subList(fromIndex, toIndex);
	}

	

}
