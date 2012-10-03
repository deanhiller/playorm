package com.alvazan.orm.impl.meta.data.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SetProxyFetchAll<T> extends OurAbstractCollection<T> implements CacheLoadCallback, Set<T> {

	@SuppressWarnings("unused")
	private static final long serialVersionUID = 1L;
	private Set<Holder<T>> holders = new HashSet<Holder<T>>();
	
	public SetProxyFetchAll(Object owner, NoSqlSession session, MetaAbstractClass<T> classMeta, List<byte[]> keys) {
		super(owner, session, classMeta, keys);
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
