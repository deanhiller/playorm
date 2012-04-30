package com.alvazan.orm.impl.meta;

import java.util.Map;

import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer3.spi.db.Column;


public interface MetaField<OWNER> {

	public void translateFromColumn(Map<String, Column> columns, OWNER entity, NoSqlSession session);
	
	public void translateToColumn(OWNER entity, Column col);

	public void translateToIndexFormat(OWNER entity, Map<String, String> indexFormat);
}
