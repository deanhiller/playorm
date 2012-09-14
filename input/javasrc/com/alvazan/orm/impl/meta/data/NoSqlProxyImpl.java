package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.MethodHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class NoSqlProxyImpl<T> implements MethodHandler {

	private static final Logger log = LoggerFactory.getLogger(NoSqlProxyImpl.class);
	private NoSqlSession session;
	private Object entityId;
	private Method idMethod;
	private MetaAbstractClass<T> classMeta;
	private boolean isInitialized = false;
	private CacheLoadCallback cacheLoadCallback;
	private Map<Field, Object> indexFieldToOriginalValue = new HashMap<Field, Object>();
	
	public NoSqlProxyImpl(NoSqlSession session, MetaAbstractClass<T> classMeta, Object entityId, CacheLoadCallback cacheLoadCallback) {
		if(classMeta.getColumnFamily() == null)
			throw new IllegalArgumentException("column family in the classMeta parameter cannot be null");
		this.session = session;
		this.entityId = entityId;
		this.classMeta = classMeta;
		this.idMethod = classMeta.getIdField().getIdMethod();
		this.cacheLoadCallback = cacheLoadCallback;
	}
	
	/**
	 * @param self - The proxy object(if you call any method on self, it will result in calling invoke method
	 *                   AND that includes a simple toString like if you did log.info("proxy="+self);
	 * @param superClassMethod - The method that is on the superclass like Account.java
	 * @param subclassProxyMethod - The method that is on the proxy like Account_$$_javassist_0
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(Object selfArg, Method superClassMethod, Method subclassProxyMethod, Object[] args)
			throws Throwable {
		T self = (T)selfArg;
		if(log.isTraceEnabled()) {
			log.trace("name="+superClassMethod.getName()+"  superClass type="+superClassMethod.getDeclaringClass());
			log.trace("name="+subclassProxyMethod.getName()+" proxy type="+subclassProxyMethod.getDeclaringClass());
		}
		
		//Here we shortcut as we do not need to go to the database...
		if(idMethod.equals(superClassMethod))
			return entityId;
		else if("__markInitializedAndCacheIndexedValues".equals(superClassMethod.getName())) {
			cacheIndexedValues(self);
			isInitialized = true;
			return null;
		} else if("__getOriginalValues".equals(superClassMethod.getName())) {
			return getOriginalValues();
		}
			
		
		//Any other method that is called, toString, getHashCode, getName, someMethod() all end up
		//loading the objects fields from the database in case those methods use those fields
		if(!isInitialized) {
			//If we have a cacheLoadCallback from a List or Map, we are not just loading
			//this entity but the callback method will load this and all other entities from
			//the database in ONE single call instead.
			if(cacheLoadCallback != null) {
				cacheLoadCallback.loadCacheIfNeeded();
			} else {
				fillInThisOneInstance(self);
			}
			
			isInitialized = true;
		}
		
		//Not sure if this should be subclassProxyMethod or superClassMethod
        return subclassProxyMethod.invoke(self, args);  // execute the original method.
	}

	private Map<Field, Object> getOriginalValues() {
		return indexFieldToOriginalValue;
	}

	private void cacheIndexedValues(T self) {
		List<MetaField<T>> cols = classMeta.getIndexedColumns();
		for(MetaField<T> f : cols) {
			Field field = f.getField();
			Object value = f.getFieldRawValue(self);
			indexFieldToOriginalValue.put(field, value);
		}
	}

	private void fillInThisOneInstance(T self) {
		MetaIdField<T> idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		byte[] rowKey = converter.convertToNoSql(entityId);
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		
		String cf = classMeta.getColumnFamily();
		AbstractCursor<KeyValue<Row>> rows = session.find(cf, rowKeys, false, null);
		Holder<KeyValue<Row>> holder = rows.nextImpl();
		if(holder == null)
			throw new RowNotFoundException("row for type="+classMeta.getMetaClass().getName()+" not found for key="+entityId);
		KeyValue<Row> next = holder.getValue();
		if(next.getValue() == null)
			throw new RowNotFoundException("row for type="+classMeta.getMetaClass().getName()+" not found for key="+entityId);
		
		Row row = next.getValue();
		classMeta.fillInInstance(row, session, self);
	}

}
