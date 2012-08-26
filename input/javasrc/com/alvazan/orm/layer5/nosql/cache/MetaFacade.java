package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;

public interface MetaFacade {

	DboTableMeta getColumnFamily(String tableName);

	DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName);

}
