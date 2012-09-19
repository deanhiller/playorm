package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;

public final class MetaCursorField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private MetaAbstractClass<PROXY> classMeta;
	private Field fieldForKey;
	private DboColumnToManyMeta metaDbo = new DboColumnToManyMeta();
	
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public void translateFromColumn(Row row, OWNER entity,
			NoSqlSession session) {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public Object fetchField(Object entity) {
		throw new UnsupportedOperationException("only used for partitioning and multivalue column can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}

	@Override
	public String translateToString(Object fieldsValue) {
		throw new UnsupportedOperationException("only used for partitioning and multievalue column can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}
	
	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		throw new UnsupportedOperationException("not done yet");
	}

	public void setup(DboTableMeta tableMeta, Field field, String colName, MetaAbstractClass<PROXY> classMeta, Field fieldForKey) {
		DboTableMeta fkToTable = classMeta.getMetaDbo();
		metaDbo.setup(tableMeta, colName, fkToTable, true);
		super.setup(field, colName);
		this.classMeta = classMeta;
		this.fieldForKey = fieldForKey;
	}

	@Override
	public String toString() {
		return "MetaCursorField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()
				+ "<"+classMeta.getMetaClass().getName()+">), columnName=" + columnName + "]";
	}

	@Override
	public byte[] translateValue(Object value) {
		throw new UnsupportedOperationException("Bug, this operation shold never be called for lists");
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		throw new UnsupportedOperationException("Bug, this should never be called");
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		throw new UnsupportedOperationException("Bug, this should never be called");		
	}
}
