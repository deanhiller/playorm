package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;

public class MetaCommonField<OWNER> extends MetaAbstractField<OWNER> {
	
	private Converter converter;

	@Override
	public String toString() {
		return "MetaCommonField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Column column, OWNER entity, NoSqlSession session) {
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
		super.setup(field2, colName, null, field2.getType(), false);
		this.converter = converter;
	}

	@Override
	public String translateToIndexFormat(OWNER entity) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		String indexValue = converter.convertToIndexFormat(value);
		return indexValue;
	}

	public Class<?> getFieldType(){
		return this.field.getType();
	}

	@Override
	public String translateIfEntity(Object value) {
		return value+"";
	}

}
