package com.alvazan.orm.impl.meta.data.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.alvazan.orm.api.z8spi.conv.Converter;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class SimpleAbstractCollection<T> implements Collection<T> {

	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
	
	private boolean removeAll;
	protected Set<T> added = new HashSet<T>();
	private List<T> originalValues = new ArrayList<T>();
	protected List<T> values = new ArrayList<T>();

    public SimpleAbstractCollection(Converter converter, List<byte[]> values) {
    	for(byte[] val : values) {
    		T obj = (T) converter.convertFromNoSql(val);
    		this.values.add(obj);
    		originalValues.add(obj);
    	}
	}
    
	//Callback from one of the proxies to load the entire cache based
//	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
//	public void loadCacheIfNeeded() {
//		if(cacheLoaded)
//			return;
//		
//		DboTableMeta metaDbo = metaClass.getMetaDbo();
//		DboColumnIdMeta idMeta = metaDbo.getIdColumnMeta();
//		Iterable<byte[]> virtKeys = new IterToVirtual(metaDbo, keys);
//		AbstractCursor<KeyValue<Row>> rows = session.find(metaDbo, virtKeys, false, null);
//		int counter = 0;
//		while(true) {
//			com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> holder = rows.nextImpl();
//			if(holder == null)
//				break;
//			KeyValue<Row> kv = holder.getValue();
//			byte[] key = (byte[]) kv.getKey();
//			byte[] nonVirtKey = idMeta.unformVirtRowKey(key);
//			Row row = kv.getValue();
//			Tuple<T> tuple = metaClass.convertIdToProxy(row, session, nonVirtKey, null);
//			if(row == null) {
//				throw new IllegalStateException("This entity is corrupt(your entity='"+owner+"') and contains a" +
//						" reference/FK to a row that does not exist in another table.  " +
//						"It refers to another entity with pk="+tuple.getEntityId()+" which does not exist");
//			}
//			Holder<T> h = (Holder) originalHolders.get(counter);
//			T value = h.getValue();
//			if(value instanceof NoSqlProxy) {
//				//inject the row into the proxy object here to load it's fields
//				metaClass.fillInInstance(row, session, value);
//				//((NoSqlProxy)value).__injectData(row);
//			}
//			counter++;
//		}
//		cacheLoaded = true;
//	}
//    
	@Override
	public int size() {
		return values.size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}
	
	@Override
	public boolean contains(Object o) {
		return values.contains(o);
	}
	
    public <E> E[] toArray(E[] a) {
        // Estimate size of array; be prepared to see more or fewer elements
        int size = size();
        E[] r = a;
        if(a.length < size)
        	r = (E[])java.lang.reflect.Array
                    .newInstance(a.getClass().getComponentType(), size);
        
        Iterator<T> it = values.iterator();

        for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) { // fewer elements than expected
                if (a != r)
                    return Arrays.copyOf(r, i);
                r[i] = null; // null-terminate
                return r;
            }
            r[i] = (E)it.next();
        }
        
        if(it.hasNext())
        	return finishToArray(r, it);
        return r;
    }
    
    public Object[] toArray() {
        // Estimate size of array; be prepared to see more or fewer elements
        T[] r = (T[]) new Object[size()];
        Iterator<T> it = values.iterator();
        for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) // fewer elements than expected
                return Arrays.copyOf(r, i);
            r[i] = it.next();
        }
        if(it.hasNext())
        	return finishToArray(r, it);
        return r;
    }

	private static <E> E[] finishToArray(E[] r2, Iterator it2) {
		E[] r = r2;
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
        if(i == r.length)
        	return r;
        return Arrays.copyOf(r, i);
    }
    
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError
                ("Required array size too large");
        if(minCapacity > MAX_ARRAY_SIZE)
        	return Integer.MAX_VALUE;
        return MAX_ARRAY_SIZE;
    }
    
	private class OurItr implements Iterator<T> {
		private Iterator delegate;
		private Object lastReturned;

		public OurItr(Iterator iter) {
			delegate = iter;
		}
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public T next() {
			Object holder = delegate.next();
			lastReturned = holder;
			return (T) holder;
		}

		@Override
		public void remove() {
			added.remove(lastReturned);
			delegate.remove();
		}
	}
	
	@Override
	public Iterator<T> iterator() {
		return new OurItr(values.iterator());
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return values.containsAll(c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return added.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return added.retainAll(c);
	}

	@Override
	public void clear() {
		removeAll = true;
		added.clear();
		values.clear();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		return added.addAll(c);
	}
	
	@Override
	public boolean add(T e) {
		added.add(e);
		return values.add(e);
	}

	@Override
	public boolean remove(Object o) {
		added.remove(o);
		return values.remove(o);
	}

	public Collection<T> getToBeRemoved() {
		List<T> removes = new ArrayList<T>();
		Collection<T> current = values;
		for(T holder : originalValues) {
			if(!current.contains(holder))
				removes.add(holder);
		}
		return removes;
	}

	public Collection<T> getToBeAdded() {
		return added;
	}
}
