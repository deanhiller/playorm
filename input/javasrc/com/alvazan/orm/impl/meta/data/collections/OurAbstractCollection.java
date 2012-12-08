package com.alvazan.orm.impl.meta.data.collections;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.IterToVirtual;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;
import com.alvazan.orm.impl.meta.data.Tuple;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class OurAbstractCollection<T> implements Collection<T>, CacheLoadCallback {

	private static final Logger log = LoggerFactory.getLogger(OurAbstractCollection.class);
	
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
	
	private NoSqlSession session;
	protected MetaAbstractClass<T> metaClass;
	
	private List<byte[]> keys;
	protected List<Holder<T>> originalHolders = new ArrayList<Holder<T>>();
	private boolean cacheLoaded = false;

	private boolean removeAll;
	//cannot be a Set or we may get nullpointer as sometimes the key does not exist yet and doing
	//key.hashCode results in null as adding to a HashSet will use hashCode and equals while List does not
	protected List<Holder<T>> added = new ArrayList<Holder<T>>();

	private Object owner;
	private String field;
	
    public OurAbstractCollection(Object owner, NoSqlSession session2, MetaAbstractClass<T> classMeta2, List<byte[]> keys, Field field) {
		this.session = session2;
		this.metaClass = classMeta2;
		this.owner = owner;
		this.keys = keys;
		
		this.field = field.getDeclaringClass().getSimpleName();
		this.field += "."+field.getName();
	}
    
	protected abstract Collection<Holder<T>> getHolders();
    
	//Callback from one of the proxies to load the entire cache based
	//on a hit of getXXXXX (except for getId which doesn't need to go to database)
	public void loadCacheIfNeeded() {
		if(cacheLoaded)
			return;
		
		DboTableMeta metaDbo = metaClass.getMetaDbo();
		DboColumnIdMeta idMeta = metaDbo.getIdColumnMeta();
		Iterable<byte[]> virtKeys = new IterToVirtual(metaDbo, keys);
		AbstractCursor<KeyValue<Row>> rows = session.find(metaDbo, virtKeys, false, true, null);
		String name = getClass().getSimpleName();
		log.info(name+":just loaded rows for keylist(next convert to proxies)="+keys.size()+" for field="+field);
		int counter = 0;
		while(true) {
			com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> holder = rows.nextImpl();
			if(holder == null)
				break;
			KeyValue<Row> kv = holder.getValue();
			byte[] key = (byte[]) kv.getKey();
			byte[] nonVirtKey = idMeta.unformVirtRowKey(key);
			Row row = kv.getValue();
			Tuple<T> tuple = metaClass.convertIdToProxy(row, session, nonVirtKey, null);
			if(row == null) {
				throw new IllegalStateException("This entity is corrupt(your entity='"+owner+"') and contains a" +
						" reference/FK to a row that does not exist in another table.  " +
						"It refers to another entity with pk="+tuple.getEntityId()+" which does not exist");
			}
			Holder<T> h = (Holder) originalHolders.get(counter);
			T value = h.getValue();
			if(value instanceof NoSqlProxy) {
				//inject the row into the proxy object here to load it's fields
				metaClass.fillInInstance(row, session, value);
				//((NoSqlProxy)value).__injectData(row);
			}
			counter++;
		}
		cacheLoaded = true;
	}
    
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
		Holder<T> h = new Holder<T>(metaClass, (T) o);
		return getHolders().contains(h);
	}
	
    public <E> E[] toArray(E[] a) {
        // Estimate size of array; be prepared to see more or fewer elements
        int size = size();
        E[] r = a;
        if(a.length < size)
        	r = (E[])java.lang.reflect.Array
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
        
        if(it.hasNext())
        	return finishToArray(r, it);
        return r;
    }
    
    public Object[] toArray() {
        // Estimate size of array; be prepared to see more or fewer elements
        T[] r = (T[]) new Object[size()];
        Iterator<Holder<T>> it = getHolders().iterator();
        for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) // fewer elements than expected
                return Arrays.copyOf(r, i);
            r[i] = it.next().getValue();
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
		private Holder lastReturned;

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
			lastReturned = holder;
			return (T) holder.getValue();
		}

		@Override
		public void remove() {
			added.remove(lastReturned);
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
			Holder h = new Holder(metaClass, val);
			holders.add(h);
		}
		return holders;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		loadCacheIfNeeded();
		Collection holdersColl = createHolders(c);
		added.removeAll(holdersColl);
		return getHolders().removeAll(holdersColl);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		loadCacheIfNeeded();
		Collection holdersColl = createHolders(c);
		added.retainAll(holdersColl);
		return getHolders().retainAll(holdersColl);
	}

	@Override
	public void clear() {
		removeAll = true;
		added.clear();
		getHolders().clear();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Collection holdersColl = createHolders(c);
		added.addAll(holdersColl);
		return getHolders().addAll(holdersColl);
	}
	
	@Override
	public boolean add(T e) {
		Holder<T> h = new Holder<T>(metaClass, e);
		added.add(h);
		return getHolders().add(h);
	}

	@Override
	public boolean remove(Object o) {
		loadCacheIfNeeded(); //we have to do this so equals will work
		Holder<T> h = new Holder<T>(metaClass, (T) o);
		added.remove(h);
		return getHolders().remove(h);
	}

	public Collection<T> getToBeRemoved() {
		List<T> removes = new ArrayList<T>();
		if(!removeAll && !cacheLoaded)
			return removes;

		//if they removed all, they coudl have added some back so here we check for what was
		//truly removed in the end
		Collection<Holder<T>> current = getHolders();
		for(Holder<T> holder : originalHolders) {
			if(!current.contains(holder))
				removes.add(holder.getValue());
		}
		return removes;
	}

	public Collection<T> getToBeAdded() {
		List<T> adds = new ArrayList<T>();
		for(Holder<T> h : added) {
			adds.add(h.getValue());
		}
		return adds;
	}
}
