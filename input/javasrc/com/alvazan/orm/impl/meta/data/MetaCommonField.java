package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.conv.Converter;

public class MetaCommonField<OWNER> extends MetaAbstractField<OWNER> {
	
	private Converter converter;

	@Override
	public String toString() {
		return "MetaCommonField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		String columnName = getColumnName();
		Column column = row.getColumn(columnName.getBytes());
		
		if(column == null) {
			column = new Column();
		}
		
		Object value = converter.convertFromNoSql(column.getValue());
		ReflectionUtil.putFieldValue(entity, field, value);
	}
	
	public void translateToColumn(OWNER entity, RowToPersist row) {
		Column col = new Column();
		row.getColumns().add(col);
		
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
