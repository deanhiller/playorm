package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javassist.util.proxy.MethodHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.base.exc.RowNotFoundException;
import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class NoSqlProxyImpl<T> implements MethodHandler {

	private static final Logger log = LoggerFactory.getLogger(NoSqlProxyImpl.class);
	private NoSqlSession session;
	private Object entityId;
	private Method idMethod;
	private MetaClass<T> classMeta;
	private boolean isInitialized = false;
	private CacheLoadCallback cacheLoadCallback;
	
	public NoSqlProxyImpl(NoSqlSession session, MetaClass<T> classMeta, Object entityId, CacheLoadCallback cacheLoadCallback) {
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
		else if("__injectData".equals(subclassProxyMethod.getName())) {
			//This is purely to initialize the proxies from a List/Map of proxies where we already
			//retrieved the List<Row> from the database.
			//TODO: REMOVE ME, no longer needed
			return null;
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

	private void fillInThisOneInstance(T self) {
		MetaIdField<T> idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		byte[] rowKey = converter.convertToNoSql(entityId);
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		List<Row> rows = session.find(classMeta.getColumnFamily(), rowKeys);
		if(rows.size() != 1)
			throw new RowNotFoundException("row for type="+classMeta.getMetaClass().getName()+" not found for key="+entityId);
		else if(rows.get(0) == null)
			throw new RowNotFoundException("row for type="+classMeta.getMetaClass().getName()+" not found for key="+entityId);
		
		Row row = rows.get(0);
		classMeta.fillInInstance(row, session, self);
	}

}
