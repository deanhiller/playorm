package com.alvazan.orm.layer5.query;

import java.util.Map;

import com.alvazan.orm.api.z8spi.MetaLoader;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.MetaFacade;
import com.alvazan.orm.parser.antlr.ParsedNode;

public class MetaFacadeImpl implements MetaFacade {

	private MetaLoader mgr;
	private DboDatabaseMeta metaInfo;
	private Map<String, Integer> attributeUsedCnt;
	
	public MetaFacadeImpl(MetaLoader mgr, DboDatabaseMeta metaInfo2) {
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
