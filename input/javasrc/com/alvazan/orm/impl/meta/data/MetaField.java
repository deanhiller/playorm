package com.alvazan.orm.impl.meta.data;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;


public interface MetaField<OWNER> {

	public String getColumnName();
	public String getFieldName();
	public DboColumnMeta getMetaDbo();
	
	public void translateFromColumn(Column column, OWNER entity, NoSqlSession session);
	public void translateToColumn(OWNER entity, Column col);
	
	public Class<?> getFieldType();

	//TODO: should be Map<String, Object> so we can return int, double, etc. etc.
	public Object translateToIndexFormat(OWNER entity);

}
