package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.Map;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer3.spi.Column;

public class MetaProxyField implements MetaField {

	protected Field field;
	private String columnName;
	//ClassMeta Will eventually have the idField that has the converter!!!
	//once it is scanned
	private MetaClass<?> classMeta;
	
	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Map<String, Column> columns, Object entity, NoSqlSession session) {
		Column column = columns.get(columnName);
		Object proxy = convertIdToProxy(column.getValue(), session);
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}
	
	public void translateToColumn(Object entity, Column col) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
		byte[] byteVal = convertProxyToId(value);
		col.setName(columnName);
		col.setValue(byteVal);
	}

	public Object convertIdToProxy(byte[] id, NoSqlSession session) {
		if(id == null)
			return null;
		MetaIdField idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		Object entityId = converter.convertFromNoSql(id);
		Object proxy = createProxy(entityId, session);
		ReflectionUtil.putFieldValue(proxy, idField.getField(), entityId);
		return proxy;
	}
	
	private Object createProxy(Object entityId, NoSqlSession session) {
		Class<?> subclassProxyClass = classMeta.getProxyClass();
		Proxy inst = (Proxy) ReflectionUtil.create(subclassProxyClass);
		inst.setHandler(new NoSqlProxyImpl(session, classMeta, entityId));
		return inst;
	}

	byte[] convertProxyToId(Object value) {
		if(value == null)
			return null;
		MetaIdField idField = classMeta.getIdField();
		Object id = ReflectionUtil.fetchFieldValue(value, idField.getField());
		Converter converter = idField.getConverter();
		return converter.convertToNoSql(id);
	}	
	
	void setup(Field field2, String colName, MetaClass<?> classMeta) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.classMeta = classMeta;
	}
}
