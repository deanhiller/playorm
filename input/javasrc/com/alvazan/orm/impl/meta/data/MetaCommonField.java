package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.TypeEnum;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;
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
	
	public void translateToColumn(OWNER entity, RowToPersist row, String columnFamily) {
		Column col = new Column();
		row.getColumns().add(col);

		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		byte[] byteVal = translateValue(value);
		col.setName(columnName.getBytes());
		col.setValue(byteVal);
		
		TypeEnum storageType = getMetaDbo().getStorageType();
		addIndexInfo(entity, row, columnFamily, value, byteVal, storageType);
	}
	
	@Override
	public byte[] translateValue(Object value) {
		return converter.convertToNoSql(value);
	}
	
	public void setup(Field field2, String colName, Converter converter, String indexPrefix) {
		super.setup(field2, colName, null, field2.getType(), false, indexPrefix);
		this.converter = converter;
	}

	public Class<?> getFieldType(){
		return this.field.getType();
	}
	

}
