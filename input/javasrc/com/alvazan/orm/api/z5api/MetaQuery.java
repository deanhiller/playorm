package com.alvazan.orm.api.z5api;

import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypeInfo;



public interface MetaQuery<T> {

	SpiQueryAdapter createSpiMetaQuery(NoSqlSession session);

	TypeInfo getMetaFieldByParameter(String name);

	DboTableMeta getTargetTable();

	String getQuery();
	
}
