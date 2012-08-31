package com.alvazan.orm.layer5.nosql.cache;

import java.util.Map;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.parser.antlr.MetaFacade;
import com.alvazan.orm.parser.antlr.ParsedNode;

public class MetaFacadeImpl implements MetaFacade {

	private NoSqlEntityManager mgr;
	private DboDatabaseMeta metaInfo;
	private Map<String, Integer> attributeUsedCnt;
	
	public MetaFacadeImpl(NoSqlEntityManager mgr, DboDatabaseMeta metaInfo2) {
		this.mgr = mgr;
		this.metaInfo = metaInfo2;
	}

	@Override
	public ParsedNode createExpression(int nodeType) {
		return new ExpressionNode(nodeType);
	}
	
	@Override
	public DboTableMeta getColumnFamily(String tableName) {
		DboTableMeta metaClass = metaInfo.getMeta(tableName);
		if(metaClass == null && mgr != null)
			metaClass = mgr.find(DboTableMeta.class, tableName);
		return metaClass;
	}

	@Override
	public DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName) {
		return metaClass.getColumnMeta(columnName);
	}

	@Override
	public DboColumnMeta getFkMetaIfExist(DboTableMeta tableMeta, String column) {
		DboColumnMeta columnMeta = tableMeta.getColumnMeta(column);
		return columnMeta;
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
