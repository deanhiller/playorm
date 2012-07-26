package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;

public abstract class MetaAbstractField<OWNER> implements MetaField<OWNER> {
	private DboColumnMeta metaDbo = new DboColumnMeta();
	protected Field field;
	protected String columnName;
	
	public String getColumnName() {
		return columnName;
	}
	@Override
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public final String getFieldName() {
		return field.getName();
	}
	
	@SuppressWarnings("rawtypes")
	public void setup(Field field2, String colName, DboTableMeta fkToTable, Class classType, boolean isToManyColumn) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		metaDbo.setup(columnName, fkToTable, classType, isToManyColumn);		
	}
	
	
}
