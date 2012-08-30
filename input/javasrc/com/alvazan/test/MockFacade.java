package com.alvazan.test;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi3.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.nosql.cache.MetaFacade;
import com.alvazan.orm.parser.antlr.ParsedNode;

public class MockFacade implements MetaFacade {

	private Map<String, DboTableMeta> nameToTable = new HashMap<String, DboTableMeta>();
	
	@Override
	public DboTableMeta getColumnFamily(String tableName) {
		DboTableMeta existing = nameToTable.get(tableName);
		if(existing == null) {
			existing = new DboTableMeta();
			existing.setColumnFamily(tableName);
			nameToTable.put(tableName, existing);
		}
		return existing;
	}

	@Override
	public DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName) {
		DboColumnMeta colMeta = metaClass.getColumnMeta(columnName);
		if(colMeta == null) {
			DboColumnCommonMeta temp = new DboColumnCommonMeta();
			temp.setup(metaClass, columnName, null, true, false);
			metaClass.addColumnMeta(temp);
			colMeta = temp;
		}
			
		return colMeta;
	}

	@Override
	public DboColumnMeta getFkMetaIfExist(DboTableMeta tableMeta, String column) {
		DboTableMeta fkToTable = new DboTableMeta();
		fkToTable.setColumnFamily("fktable"+System.currentTimeMillis());
		DboColumnToOneMeta toOne = new DboColumnToOneMeta();
		toOne.setup(tableMeta, column, fkToTable, true, false);
		return toOne;
	}

	@Override
	public ParsedNode createExpression(int nodeType) {
		return new ExpressionNode(nodeType);
	}

	
}
