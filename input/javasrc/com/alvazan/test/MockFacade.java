package com.alvazan.test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.MetaFacade;
import com.alvazan.orm.parser.antlr.ParsedNode;

public class MockFacade implements MetaFacade {

	private Map<String, DboTableMeta> nameToTable = new HashMap<String, DboTableMeta>();
	private Map<String, Integer> attributeUsedCnt;
	
	@Override
	public DboTableMeta getColumnFamily(String tableName) {
		DboTableMeta existing = nameToTable.get(tableName);
		if(existing == null) {
			existing = createTableMeta(tableName);
			nameToTable.put(tableName, existing);
		}
		return existing;
	}

	private DboTableMeta createTableMeta(String tableName) {
		DboTableMeta existing;
		existing = new DboTableMeta();
		existing.setup(null, tableName, false);
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(existing, "id", BigDecimal.class, false);
		return existing;
	}

	@Override
	public DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName) {
		DboColumnMeta colMeta = metaClass.getColumnMeta(columnName);
		if(colMeta == null) {
			DboColumnCommonMeta temp = new DboColumnCommonMeta();
			temp.setup(metaClass, columnName, String.class, true, false);
			metaClass.addColumnMeta(temp);
			colMeta = temp;
		}
			
		return colMeta;
	}

	@Override
	public DboColumnMeta getFkMetaIfExist(DboTableMeta tableMeta, String column) {
		DboTableMeta fkToTable = createTableMeta("fktable"+System.currentTimeMillis());
		DboColumnToOneMeta toOne = new DboColumnToOneMeta();
		toOne.setup(tableMeta, column, fkToTable, true, false);
		return toOne;
	}

	@Override
	public ParsedNode createExpression(int nodeType) {
		return new ExpressionNode(nodeType);
	}

	@Override
	public Map<String, Integer> getAttributeUsedCount() {
		return attributeUsedCnt;
	}

	@Override
	public void setAttributeUserCount(Map<String, Integer> attributeUsedCount) {
		attributeUsedCnt = attributeUsedCount;
	}

	
}
