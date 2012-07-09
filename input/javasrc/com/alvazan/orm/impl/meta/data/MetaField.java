package com.alvazan.orm.impl.meta.data;

import java.util.Map;

import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.MetaColumnDbo;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;


public interface MetaField<OWNER> {

	public void translateFromColumn(Map<String, Column> columns, OWNER entity, NoSqlSession session);
	
	public void translateToColumn(OWNER entity, Column col);
	
	public String getFieldName();
	
	public Class<?> getFieldType();

	//TODO: should be Object instead of String probably later so we can return int, double, etc.
	public String translateIfEntity(Object value);
	//TODO: should be Map<String, Object> so we can return int, double, etc. etc.
	public void translateToIndexFormat(OWNER entity, Map<String, String> indexFormat);
	
	public MetaColumnDbo getMetaDbo();

}
