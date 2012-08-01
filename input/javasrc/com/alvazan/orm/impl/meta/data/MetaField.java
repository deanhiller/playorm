package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Row;

public interface MetaField<OWNER> {

	public String getColumnName();

	public DboColumnMeta getMetaDbo();
	
	/**
	 * For when we are translating from nosql row, this is called on each field/column to translate
	 */
	public void translateFromColumn(Row column, OWNER entity, NoSqlSession session);
	/**
	 * For when we are translating TO nosql row, this is called on each field/column
	 */
	public void translateToColumn(OWNER entity, RowToPersist col, String columnFamily, Map<Field, Object> fieldToValue);
	
	public Field getField();

	public Object getFieldRawValue(OWNER entity);
	
	/**
	 * For when Query.setParameter is called, we need to translate into byte[] to search the index for the value
	 * @param value
	 * @return
	 */
	public byte[] translateValue(Object value);
}
