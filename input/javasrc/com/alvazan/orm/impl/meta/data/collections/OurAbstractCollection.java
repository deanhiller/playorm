package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public abstract class OurAbstractCollection<T> implements Collection<T> {

	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
	
	@Override
	public int size() {
		return getHolders().size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}
	
	@Override
	public boolean contains(Object o) {
		Holder<T> h = new Holder<T>((T) o);
		return getHolders().contains(h);
	}
	
    public <E> E[] toArray(E[] a) {
        // Estimate size of array; be prepared to see more or fewer elements
        int size = size();
        E[] r = a.length >= size ? a :
                  (E[])java.lang.reflect.Array
                  .newInstance(a.getClass().getComponentType(), size);
        Iterator<Holder<T>> it = getHolders().iterator();

        for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) { // fewer elements than expected
                if (a != r)
                    return Arrays.copyOf(r, i);
                r[i] = null; // null-terminate
                return r;
            }
            r[i] = (E)it.next().getValue();
        }
        return it.hasNext() ? finishToArray(r, it) : r;
    }
    
    public Object[] toArray() {
    	Iterator<Holder<T>> iterator = getHolders().iterator();
        // Estimate size of array; be prepared to see more or fewer elements
        T[] r = (T[]) new Object[size()];
        Iterator<Holder<T>> it = getHolders().iterator();
        for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) // fewer elements than expected
                return Arrays.copyOf(r, i);
            r[i] = it.next().getValue();
        }
        return it.hasNext() ? finishToArray(r, it) : r;
    }

    protected abstract Collection<Holder<T>> getHolders();

	private static <E> E[] finishToArray(E[] r, Iterator it2) {
		Iterator<Holder<E>> it = it2;
        int i = r.length;
        while (it.hasNext()) {
            int cap = r.length;
            if (i == cap) {
                int newCap = cap + (cap >> 1) + 1;
                // overflow-conscious code
                if (newCap - MAX_ARRAY_SIZE > 0)
                    newCap = hugeCapacity(cap + 1);
                r = Arrays.copyOf(r, newCap);
            }
            r[i++] = (E)it.next().getValue();
        }
        // trim if overallocated
        return (i == r.length) ? r : Arrays.copyOf(r, i);
    }
    
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError
                ("Required array size too large");
        return (minCapacity > MAX_ARRAY_SIZE) ?
            Integer.MAX_VALUE :
            MAX_ARRAY_SIZE;
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
		return new OurItr(getHolders().iterator());
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		Collection holdersColl = createHolders(c);
		return getHolders().containsAll(holdersColl);
	}
	
	protected Collection createHolders(Collection c) {
		Collection holders = new ArrayList();
		for(Object val : c) {
			Holder h = new Holder(val);
			holders.add(h);
		}
		return holders;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		loadCacheIfNeeded();
		Collection holdersColl = createHolders(c);
		return getHolders().removeAll(holdersColl);
	}

	protected abstract void loadCacheIfNeeded();

	@Override
	public boolean retainAll(Collection<?> c) {
		loadCacheIfNeeded();
		Collection holdersColl = createHolders(c);
		return getHolders().retainAll(holdersColl);
	}

	@Override
	public void clear() {
		getHolders().clear();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Collection holdersColl = createHolders(c);
		return getHolders().addAll(holdersColl);
	}
	
	@Override
	public boolean add(T e) {
		Holder<T> h = new Holder<T>(e);
		return getHolders().add(h);
	}

	@Override
	public boolean remove(Object o) {
		loadCacheIfNeeded(); //we have to do this so equals will work
		Holder h = new Holder(o);
		return getHolders().remove(h);
	}
}
