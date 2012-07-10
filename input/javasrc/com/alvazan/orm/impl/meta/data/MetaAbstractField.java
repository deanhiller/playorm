package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.spi.layer2.MetaColumnDbo;
import com.alvazan.orm.api.spi.layer2.MetaTableDbo;

public abstract class MetaAbstractField<OWNER> implements MetaField<OWNER> {
	private MetaColumnDbo metaDbo = new MetaColumnDbo();
	protected Field field;
	protected String columnName;
	
	public String getColumnName() {
		return columnName;
	}
	@Override
	public MetaColumnDbo getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public final String getFieldName() {
		return field.getName();
	}
	
	public void setup(Field field2, String colName, MetaTableDbo fkToTable, Class classType, boolean isToManyColumn) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		metaDbo.setup(columnName, fkToTable, classType, isToManyColumn);		
	}
	
	
}
