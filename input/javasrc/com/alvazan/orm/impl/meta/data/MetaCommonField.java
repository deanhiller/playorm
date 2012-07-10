package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.MetaColumnDbo;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;

public class MetaCommonField<OWNER> implements MetaField<OWNER> {
	
	private MetaColumnDbo metaDbo = new MetaColumnDbo();
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

	public void setup(Field field2, String colName, Converter converter) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		this.converter = converter;
		metaDbo.setup(columnName, null, field.getType(), false);
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

	@Override
	public MetaColumnDbo getMetaDbo() {
		return metaDbo;
	}
}
