package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.StorageTypeEnum;

public abstract class MetaAbstractField<OWNER> implements MetaField<OWNER> {
	private DboColumnMeta metaDbo = new DboColumnMeta();
	protected Field field;
	protected String columnName;
	
	public String getColumnName() {
		return columnName;
	}
	@Override
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public final String getFieldName() {
		return field.getName();
	}
	
	@SuppressWarnings("rawtypes")
	public void setup(Field field2, String colName, DboTableMeta fkToTable, Class classType, boolean isToManyColumn, String indexPrefix) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
		metaDbo.setup(columnName, fkToTable, classType, isToManyColumn, indexPrefix);
	}
	
	protected void addIndexInfo(OWNER entity, RowToPersist row,
			String columnFamily, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		if(!this.getMetaDbo().isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		else if(entity instanceof Proxy) {
			//this is for removals when indexing is changing
			throw new UnsupportedOperationException("not supported just yet, we need to code up the removals");
		}
		byte[] pk = row.getKey();

		IndexData data = new IndexData();
		data.setColumnFamilyName(storageType.getIndexTableName());
		data.setRowKey("/"+columnFamily+"/"+getMetaDbo().getColumnName());
		data.setIndexedValue(byteVal);
		data.setPrimaryKey(pk);
		data.setIndexedValueType(storageType);
		row.addIndexToPersist(data);
	}
	
}
