package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.KeyValue;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.Column;

public class MetaClass<T> {

	private Class<T> metaClass;
	//This is a dynamic class using NoSqlProxyImpl.java as the invocationhandler and
	//will be a subclass of metaClass field above!!
	private Class<T> proxyClass;
	
	private String columnFamily;
	
	private MetaIdField<T> idField;
	private List<MetaField<T>> fields = new ArrayList<MetaField<T>>();

	public Object fetchId(Object entity) {
		return ReflectionUtil.fetchFieldValue(entity, idField.getField());
	}
	
	public byte[] convertIdToNoSql(Object entityId) {
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(entityId);
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
		for(MetaField<T> field : fields) {
			field.translateFromColumn(columns, inst, session);
		}
		
		return key;
	}
	
	public RowToPersist translateToRow(T entity) {
		RowToPersist row = new RowToPersist();
		idField.translateToRow(entity, row);
		
		for(MetaField<T> m : fields) {
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
	void setColumnFamily(String colFamily) {
		if(colFamily == null)
			throw new IllegalArgumentException("colFamily cannot be null");
		this.columnFamily = colFamily;
	}

	void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	Class<T> getMetaClass() {
		return metaClass;
	}
	
	void addMetaField(MetaField<T> field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		fields.add(field);
	}

	void setIdField(MetaIdField<T> field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		this.idField = field;
	}

	public MetaIdField<T> getIdField() {
		return idField;
	}
	
	void setProxyClass(Class<T> proxyClass) {
		this.proxyClass = proxyClass;
	}

	Class<?> getProxyClass() {
		return proxyClass;
	}

}
