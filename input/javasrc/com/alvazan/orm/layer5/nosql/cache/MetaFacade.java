package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.parser.antlr.ParsedNode;

public interface MetaFacade {

	DboTableMeta getColumnFamily(String tableName);

	DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName);

	DboColumnMeta getFkMetaIfExist(DboTableMeta tableMeta, String column);

	ParsedNode createExpression(int nodeType);

}
