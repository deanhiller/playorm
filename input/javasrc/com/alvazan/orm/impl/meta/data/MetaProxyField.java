package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.query.MetaClassDbo;
import com.alvazan.orm.impl.meta.query.MetaFieldDbo;

public class MetaProxyField<OWNER, PROXY> implements MetaField<OWNER> {

	private MetaFieldDbo metaDbo = new MetaFieldDbo();
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
	
	public void setup(Field field2, String colName, MetaClass<PROXY> classMeta) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.classMeta = classMeta;
		
		MetaClassDbo fkToTable = classMeta.getMetaDbo();
		metaDbo.setup(colName, fkToTable, field.getType().getName());
	}

	@Override
	public void translateToIndexFormat(OWNER entity,
			Map<String, String> indexFormat) {
		String idStr = translateIfEntity(entity);
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

	@SuppressWarnings("unchecked")
	@Override
	public String translateIfEntity(Object entity) {
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		MetaIdField<PROXY> idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		String idStr = converter.convertToIndexFormat(value);
		return idStr;
	}

	@Override
	public MetaFieldDbo getMetaDbo() {
		return metaDbo;
	}
}
