package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer3.spi.db.Column;

public class MetaProxyField<OWNER, PROXY> implements MetaField<OWNER> {

	protected Field field;
	private String columnName;
	//ClassMeta Will eventually have the idField that has the converter!!!
	//once it is scanned
	private MetaClass<PROXY> classMeta;
	
	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Map<String, Column> columns, OWNER entity, NoSqlSession session) {
		Column column = columns.get(columnName);
		Object proxy = convertIdToProxy(column.getValue(), session);
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}
	
	@SuppressWarnings("unchecked")
	public void translateToColumn(OWNER entity, Column col) {
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
		byte[] byteVal = convertProxyToId(value);
		col.setName(columnName);
		col.setValue(byteVal);
	}

	public PROXY convertIdToProxy(byte[] id, NoSqlSession session) {
		if(id == null)
			return null;
		MetaIdField<PROXY> idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		Object entityId = converter.convertFromNoSql(id);
		return idField.convertIdToProxy(session, entityId);
	}
	
	byte[] convertProxyToId(PROXY value) {
		if(value == null)
			return null;
		MetaIdField<PROXY> idField = classMeta.getIdField();
		Object id = classMeta.fetchId(value);
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(id);
	}	
	
	void setup(Field field2, String colName, MetaClass<PROXY> classMeta) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.classMeta = classMeta;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void translateToIndexFormat(OWNER entity,
			Map<String, String> indexFormat) {
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		MetaIdField<PROXY> idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		String idStr = converter.convertToIndexFormat(value);
		indexFormat.put(columnName, idStr);
	}

	@Override
	public String getFieldName() {
		return this.field.getName();
	}

	@Override
	public Class<?> getFieldType() {
		return this.field.getType();
	}
}
