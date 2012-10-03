package com.alvazan.orm.impl.meta.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public abstract class MetaAbstractClass<T> implements MetaClass<T> {

	private DboTableMeta metaDbo = new DboTableMeta();
	
	private Class<T> metaClass;
	//This is a dynamic class using NoSqlProxyImpl.java as the invocationhandler and
	//will be a subclass of metaClass field above!!
	
	private String columnFamily;
	
	protected MetaIdField<T> idField;
	
	private Map<String, SpiMetaQuery> queryInfo = new HashMap<String, SpiMetaQuery>();
	
	public void setMetaDbo(DboTableMeta metaDbo) {
		this.metaDbo = metaDbo;
	}
	
	public Object fetchId(T entity) {
		if(entity == null)
			return null;
		return ReflectionUtil.fetchFieldValue(entity, idField.getField());
	}
	
//	public byte[] convertIdToNoSql(Object entityId) {
//		Converter converter = idField.getConverter();
//		return converter.convertToNoSql(entityId);
//	}
	
	@SuppressWarnings("rawtypes")
	public byte[] convertEntityToId(T value) {
		if(value == null)
			return null;
		MetaIdField idField = getIdField();
		Object id = fetchId(value);
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(id);		
	}
	
	@Override
	public String toString() {
		return "MetaClass [metaClass=" + metaClass + ", columnFamily="
				+ columnFamily + "]";
	}
	
	public String getColumnFamily() {
		return columnFamily;
	}
	public void setup(String virtualCf, String cf) {
		if(cf == null)
			throw new IllegalArgumentException("colFamily cannot be null");
		if(virtualCf != null)
			columnFamily = virtualCf;
		else
			this.columnFamily = cf;
		metaDbo.setup(virtualCf, cf);
	}

	public void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	public Class<T> getMetaClass() {
		return metaClass;
	}
	
	public void setIdField(MetaIdField<T> field) {
		this.idField = field;
	}

	public MetaIdField<T> getIdField() {
		return idField;
	}
	
	public DboTableMeta getMetaDbo() {
		return metaDbo;
	}
	
	public SpiMetaQuery getNamedQuery(String name) {
		SpiMetaQuery query = queryInfo.get(name);
		if(query == null)
			throw new IllegalArgumentException("Named query="+name+" does not exist on type="+this.metaClass.getName());
		return query;
	}

	public void addQuery(String name, SpiMetaQuery metaQuery) {
		queryInfo.put(name, metaQuery);
	}

	public abstract Class<? extends T> getProxyClass();
	public abstract Tuple<T> convertIdToProxy(Row row, NoSqlSession session, byte[] nonVirtKey, CacheLoadCallback cacheLoadCallback);
	public abstract List<MetaField<T>> getIndexedColumns();
	public abstract void fillInInstance(Row row, NoSqlSession session, T inst);

}
