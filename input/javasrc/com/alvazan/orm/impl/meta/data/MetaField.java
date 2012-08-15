package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.spi1.meta.DboColumnMeta;
import com.alvazan.orm.api.spi1.meta.IndexData;
import com.alvazan.orm.api.spi1.meta.InfoForIndex;
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
	public void translateToColumn(InfoForIndex<OWNER> info);
	
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] rowKey);
	
	public Field getField();

	public Object getFieldRawValue(OWNER entity);
	
	/**
	 * For when Query.setParameter is called, we need to translate into byte[] to search the index for the value
	 * @param value
	 * @return
	 */
	public byte[] translateValue(Object value);

	public Object fetchField(Object entity);
	public String translateToString(Object fieldsValue);
	
}
