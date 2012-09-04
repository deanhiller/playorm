package com.alvazan.orm.api.z8spi.meta;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.MethodHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class NoSqlTypedRowProxyImpl implements MethodHandler {

	private static final Logger log = LoggerFactory.getLogger(NoSqlTypedRowProxyImpl.class);
	private DboTableMeta classMeta;
	private Map<String, Object> indexFieldToOriginalValue = new HashMap<String, Object>();
	
	public NoSqlTypedRowProxyImpl(DboTableMeta classMeta) {
		if(classMeta.getColumnFamily() == null)
			throw new IllegalArgumentException("column family in the classMeta parameter cannot be null");
		this.classMeta = classMeta;
	}
	
	/**
	 * @param self - The proxy object(if you call any method on self, it will result in calling invoke method
	 *                   AND that includes a simple toString like if you did log.info("proxy="+self);
	 * @param superClassMethod - The method that is on the superclass like Account.java
	 * @param subclassProxyMethod - The method that is on the proxy like Account_$$_javassist_0
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Object invoke(Object selfArg, Method superClassMethod, Method subclassProxyMethod, Object[] args)
			throws Throwable {
		TypedRow self = (TypedRow)selfArg;
		if(log.isTraceEnabled()) {
			log.trace("name="+superClassMethod.getName()+"  superClass type="+superClassMethod.getDeclaringClass());
			log.trace("name="+subclassProxyMethod.getName()+" proxy type="+subclassProxyMethod.getDeclaringClass());
		}
		
		//Here we shortcut as we do not need to go to the database...
		if("__cacheIndexedValues".equals(superClassMethod.getName())) {
			cacheIndexedValues(self);
			return null;
		} else if("__getOriginalValues".equals(superClassMethod.getName())) {
			return getOriginalValues();
		}
			
		//Not sure if this should be subclassProxyMethod or superClassMethod
        return subclassProxyMethod.invoke(self, args);  // execute the original method.
	}

	private Map<String, Object> getOriginalValues() {
		return indexFieldToOriginalValue;
	}

	@SuppressWarnings("rawtypes")
	private void cacheIndexedValues(TypedRow self) {
		List<DboColumnMeta> cols = classMeta.getIndexedColumns();
		for(DboColumnMeta f : cols) {
			String colName = f.getColumnName();
			TypedColumn col = self.getColumn(colName);
			indexFieldToOriginalValue.put(colName, col.getValue());
		}
	}

}
