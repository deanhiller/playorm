package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.Map;

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

	public void translateFromColumn(Map<String, Column> columns, Object entity) {
		Column column = columns.get(columnName);
		Object value = classMeta.convertIdFromNoSql(column.getValue());
		ReflectionUtil.putFieldValue(entity, field, value);
	}
	
	public void translateToColumn(Object entity, Column col) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		byte[] byteVal = classMeta.convertIdToNoSql(value);
		col.setName(columnName);
		col.setValue(byteVal);
	}

	public void setup(Field field2, String colName, MetaClass<?> classMeta) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.classMeta = classMeta;
	}
}
