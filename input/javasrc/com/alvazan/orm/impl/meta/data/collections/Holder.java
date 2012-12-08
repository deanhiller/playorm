package com.alvazan.orm.impl.meta.data.collections;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.Tuple;

/**
 * A class specifically so we can account for nulls.  ie. if we don't use holders and we get
 * element[8] and it returns null, we don't know if it cached null or if we need to reprocess.  If we
 * use Holder, element[8] returning null means we have not cached it yet, otherwise it can return a
 * Holder with a null value inside it meaning we cached the null value.
 * @author dhiller
 *
 * @param <T>
 */
public class Holder<T> {

	private ByteArray key;
	private boolean hasValue;
	private T value;
	private MetaAbstractClass<T> metaClass;
	private NoSqlSession session;
	private CacheLoadCallback cacheLoadCallback;

	public Holder(MetaAbstractClass<T> metaClass, NoSqlSession session, byte[] nonVirtKey, CacheLoadCallback cb) {
		if(session != null && cb != null)
			throw new IllegalArgumentException("provide session OR cb but not both");
		this.metaClass = metaClass;
		this.session = session;
		setKey(nonVirtKey);
		this.cacheLoadCallback = cb;
	}
	public Holder(MetaAbstractClass<T> metaClass, T value) {
		this.metaClass = metaClass;
		this.value = value;
        Class<? extends Object> classType = value.getClass();
        if(metaClass.getMetaClass().isAssignableFrom(classType)) {
        	key = new ByteArray(metaClass.convertEntityToId(value));
        }
		hasValue = true;
	}
	public synchronized T getValue() {
		if(!hasValue) {
			//otherwise, we need to create and cache the value
			Tuple<T> tuple =  metaClass.convertIdToProxy(null, session, key.getKey(), cacheLoadCallback);
			T proxy = tuple.getProxy();
			value = proxy;
			hasValue = true;
		}
		return value;
	}
	
	public void setValue(T value) {
		this.value = value;
	}

	public void setKey(byte[] key) {
		this.key = new ByteArray(key);
	}

	public byte[] getKey() {
		return key.getKey();
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Holder other = (Holder) obj;

		if(key == null && other.key != null)
			return false;
		else if(key != null && other.key == null)
			return false;
		else if(other.key.equals(key))
			return true;
		
		return false;
	}
	
}
