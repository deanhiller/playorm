package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.layer3.spi.Column;

public class MetaField {

	private Field field;
	private String columnName;
	private Converter converter;

	
	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Map<String, Column> columns, Object entity) {
		Column column = columns.get(columnName);
		Object value = converter.convertFromNoSql(column.getValue());
		ReflectionUtil.putFieldValue(entity, field, value);
	}
	
	public Column translateToColumn(Object entity) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		byte[] byteVal = converter.convertToNoSql(value);
		Column col = new Column();
		col.setName(columnName);
		col.setValue(byteVal);
		return col;
	}

	void setup(Field field2, String colName, Converter converter) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.converter = converter;
	}
}
