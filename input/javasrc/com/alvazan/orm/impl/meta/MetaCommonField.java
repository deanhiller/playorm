package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer3.spi.db.Column;

public class MetaCommonField<OWNER> implements MetaField<OWNER> {
	protected Field field;
	private String columnName;
	private Converter converter;
	
	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Map<String, Column> columns, OWNER entity, NoSqlSession session) {
		Column column = columns.get(columnName);
		Object value = converter.convertFromNoSql(column.getValue());
		ReflectionUtil.putFieldValue(entity, field, value);
	}
	
	public void translateToColumn(OWNER entity, Column col) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		byte[] byteVal = converter.convertToNoSql(value);
		col.setName(columnName);
		col.setValue(byteVal);
	}

	void setup(Field field2, String colName, Converter converter) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.converter = converter;
	}

	@Override
	public void translateToIndexFormat(OWNER entity,
			Map<String, String> indexFormat) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		String indexValue = converter.convertToIndexFormat(value);
		indexFormat.put(columnName, indexValue);
	}

	@Override
	public String getFieldName() {
		return this.field.getName();
	}
	
	public Class<?> getFieldType(){
		return this.field.getType();
	}

	@Override
	public String translateIfEntity(Object value) {
		return value+"";
	}
}
