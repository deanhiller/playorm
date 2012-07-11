package com.alvazan.orm.impl.meta.data;

import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.MetaColumnDbo;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;


public interface MetaField<OWNER> {

	public String getColumnName();
	public String getFieldName();
	public MetaColumnDbo getMetaDbo();
	
	public void translateFromColumn(Column column, OWNER entity, NoSqlSession session);
	public void translateToColumn(OWNER entity, Column col);
	
	public Class<?> getFieldType();

	//TODO: should be Object instead of String probably later so we can return int, double, etc.
	public String translateIfEntity(Object value);
	//TODO: should be Map<String, Object> so we can return int, double, etc. etc.
	public Object translateToIndexFormat(OWNER entity);

}
