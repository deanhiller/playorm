package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.conv.Converter;

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
		col.setName(columnName.getBytes());
		col.setValue(byteVal);
	}

	public void setup(Field field2, String colName, Converter converter) {
		super.setup(field2, colName, null, field2.getType(), false);
		this.converter = converter;
	}

	@Override
	public Object translateToIndexFormat(OWNER entity) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		return value;
	}

	public Class<?> getFieldType(){
		return this.field.getType();
	}

	

}
