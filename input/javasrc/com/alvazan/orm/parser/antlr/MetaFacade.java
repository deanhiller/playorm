package com.alvazan.orm.parser.antlr;

import java.util.Map;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;

public interface MetaFacade {

	DboTableMeta getColumnFamily(String tableName);

	DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName);

	DboColumnMeta getFkMetaIfExist(DboTableMeta tableMeta, String column);

	ParsedNode createExpression(int nodeType);

	Map<String, Integer> getAttributeUsedCount();

	void setAttributeUserCount(Map<String, Integer> attributeUsedCount);

}
