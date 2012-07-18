package com.alvazan.orm.impl.meta.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.base.exc.PkIsNullException;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.layer2.MetaQuery;
import com.alvazan.orm.api.spi.layer2.MetaTableDbo;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class MetaClass<T> {

	public static final String IDKEY = IndexReaderWriter.IDKEY;
	
	private MetaTableDbo metaDbo = new MetaTableDbo();
	
	private Class<T> metaClass;
	//This is a dynamic class using NoSqlProxyImpl.java as the invocationhandler and
	//will be a subclass of metaClass field above!!
	private Class<? extends T> proxyClass;
	
	private String columnFamily;
	
	private MetaIdField<T> idField;
	/**
	 * This is NOT fieldName to MetaField but columnName to field(columnName can be different than
	 * fieldname)
	 */
	private Map<String,MetaField<T>> columnNameToField = new HashMap<String, MetaField<T>>();
	
	private List<MetaField<T>> indexedFields = new ArrayList<MetaField<T>>();
	private Map<String, MetaQuery<T>> queryInfo = new HashMap<String, MetaQuery<T>>();
	
	public Object fetchId(T entity) {
		return ReflectionUtil.fetchFieldValue(entity, idField.getField());
	}
	
	public byte[] convertIdToNoSql(Object entityId) {
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(entityId);
	}

	public Map<String, Object> translateForIndex(T entity) {
		Map<String, Object> item = new HashMap<String, Object>();
		Object id = fetchId(entity);
		Converter converter = idField.getConverter();
		String idStr = converter.convertToIndexFormat(id);
		if(idStr == null)
			throw new PkIsNullException("entity="+entity+" has no pk defined, so we fail " +
					"now before you called flush so nothing has been added to database nor " +
					"the index(as long as you haven't called flush yet");
		
		item.put(IDKEY, idStr);

		for(MetaField<T> field : indexedFields) {
			Object indexValue = field.translateToIndexFormat(entity);
			String columnName = field.getColumnName();
			item.put(columnName, indexValue);
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
	public Object fillInInstance(Row row, NoSqlSession session, T inst) {
		Object key = idField.translateFromRow(row, inst);

		Map<String, Column> columns = row.getColumns();
		for(MetaField<T> field : columnNameToField.values()) {
			String columnName = field.getColumnName();
			Column column = columns.get(columnName);
			field.translateFromColumn(column, inst, session);
		}
		
		return key;
	}
	
	public RowToPersist translateToRow(T entity) {
		RowToPersist row = new RowToPersist();
		idField.translateToRow(entity, row);
		
		for(MetaField<T> m : columnNameToField.values()) {
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
		metaDbo.setColumnFamily(colFamily);
	}

	public void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	public Class<T> getMetaClass() {
		return metaClass;
	}
	
	public void addMetaField(MetaField<T> field, boolean isIndexed) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		columnNameToField.put(field.getColumnName(), field);
		
		if(isIndexed)
			indexedFields.add(field);
	}
	
	public MetaField<T> getMetaFieldByCol(String columnName){
		return columnNameToField.get(columnName);
	}
	
	public  Collection<MetaField<T>> getMetaFields(){
		return this.columnNameToField.values();
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

	public MetaTableDbo getMetaDbo() {
		return metaDbo;
	}

	@SuppressWarnings("rawtypes")
	public byte[] convertProxyToId(T value) {
		if(value == null)
			return null;
		MetaIdField idField = getIdField();
		Object id = fetchId(value);
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(id);		
	}

	public T convertIdToProxy(byte[] id, NoSqlSession session, CacheLoadCallback cacheLoadCallback) {
		if(id == null)
			return null;
		MetaIdField<T> idField = this.getIdField();
		Converter converter = idField.getConverter();
		Object entityId = converter.convertFromNoSql(id);
		return idField.convertIdToProxy(session, entityId, cacheLoadCallback);
	}
}
