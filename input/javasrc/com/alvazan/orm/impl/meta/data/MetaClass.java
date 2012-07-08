package com.alvazan.orm.impl.meta.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.query.MetaClassDbo;

public class MetaClass<T> implements MetaQueryClassInfo {

	private static final String IDKEY = "id";
	
	private MetaClassDbo metaDbo = new MetaClassDbo();
	
	private Class<T> metaClass;
	//This is a dynamic class using NoSqlProxyImpl.java as the invocationhandler and
	//will be a subclass of metaClass field above!!
	private Class<? extends T> proxyClass;
	
	private String columnFamily;
	
	private MetaIdField<T> idField;
	private Map<String,MetaField<T>> fields = new HashMap<String, MetaField<T>>();
	
	private List<MetaField<T>> indexedFields = new ArrayList<MetaField<T>>();
	private Map<String, MetaQuery<T>> queryInfo = new HashMap<String, MetaQuery<T>>();
	
	public Object fetchId(T entity) {
		return ReflectionUtil.fetchFieldValue(entity, idField.getField());
	}
	
	public byte[] convertIdToNoSql(Object entityId) {
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(entityId);
	}

	public Map<String, String> translateForIndex(T entity) {
		Map<String, String> item = new HashMap<String, String>();
		Object id = fetchId(entity);
		Converter converter = idField.getConverter();
		String idStr = converter.convertToIndexFormat(id);
		
		item.put(IDKEY, idStr);
		
		for(MetaField<T> field : indexedFields) {
			field.translateToIndexFormat(entity, item);
		}
		return item;
	}
	
	public KeyValue<T> translateFromRow(Row row, NoSqlSession session) {
		T inst = ReflectionUtil.create(metaClass);
		Object key = fillInInstance(row, session, inst);
		KeyValue<T> keyVal = new KeyValue<T>();
		keyVal.setKey(key);
		keyVal.setValue(inst);
		return keyVal;
	}

	/**
	 * @param row
	 * @param session - The session to pass to newly created proxy objects
	 * @param inst The object OR the proxy to be filled in
	 * @return The key of the entity object
	 */
	Object fillInInstance(Row row, NoSqlSession session, T inst) {
		Object key = idField.translateFromRow(row, inst);

		Map<String, Column> columns = row.getColumns();
		for(MetaField<T> field : fields.values()) {
			field.translateFromColumn(columns, inst, session);
		}
		
		return key;
	}
	
	public RowToPersist translateToRow(T entity) {
		RowToPersist row = new RowToPersist();
		idField.translateToRow(entity, row);
		
		for(MetaField<T> m : fields.values()) {
			Column col = new Column();
			m.translateToColumn(entity, col);
			row.getColumns().add(col);
		}
		
		return row;
	}

	@Override
	public String toString() {
		return "MetaClass [metaClass=" + metaClass + ", columnFamily="
				+ columnFamily + "]";
	}
	
	public String getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(String colFamily) {
		if(colFamily == null)
			throw new IllegalArgumentException("colFamily cannot be null");
		this.columnFamily = colFamily;
		metaDbo.setTableName(colFamily);
	}

	public void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	public Class<T> getMetaClass() {
		return metaClass;
	}
	
	public void addMetaField(MetaField<T> field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		fields.put(field.getFieldName(), field);
	}
	
	public MetaField<T> getMetaField(String fieldName){
		return fields.get(fieldName);
	}
	
	public  Collection<MetaField<T>> getMetaFields(){
		return this.fields.values();
	}
	
	public void setIdField(MetaIdField<T> field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		this.idField = field;
	}

	public MetaIdField<T> getIdField() {
		return idField;
	}
	
	public void setProxyClass(Class<? extends T> proxyClass) {
		this.proxyClass = proxyClass;
	}

	Class<? extends T> getProxyClass() {
		return proxyClass;
	}

	public MetaQuery<T> getNamedQuery(String name) {
		MetaQuery<T> query = queryInfo.get(name);
		if(query == null)
			throw new IllegalArgumentException("Named query="+name+" does not exist on type="+this.metaClass.getName());
		return query;
	}

	public void addQuery(String name, MetaQuery<T> metaQuery) {
		queryInfo.put(name, metaQuery);
	}

	@Override
	public String getIdFieldName() {
		return idField.getField().getName();
	}

	public MetaClassDbo getMetaDbo() {
		return metaDbo;
	}
}
