package com.alvazan.orm.impl.meta;

import java.util.Map;

import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer3.spi.Column;


public interface MetaField {

	public void translateFromColumn(Map<String, Column> columns, Object entity, NoSqlSession session);
	
	public void translateToColumn(Object entity, Column col);


}
