package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.TypeEnum;

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
			String columnFamily, Object value, byte[] byteVal, TypeEnum storageType) {
		if(!this.getMetaDbo().isIndexed())
			return;
		else if(storageType == TypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes");
		else if(entity instanceof Proxy) {
			//this is for removals when indexing is changing
		}
		byte[] pk = row.getKey();
		int len = byteVal.length;
		if(len > Short.MAX_VALUE)
			throw new IllegalArgumentException("Indexed field in bytes is larger than Short.MAX_VALUE.  The value from field="+field);
		short size = (short)len;
		byte[] indexColName = new byte[byteVal.length+pk.length+2];

		copy(indexColName, byteVal, 0);
		copy(indexColName, pk, byteVal.length);
		indexColName[indexColName.length-2] = (byte)(size & 0xff);
		indexColName[indexColName.length-1] = (byte)((size >> 8) & 0xff);
		
		IndexData data = new IndexData();
		data.setColumnFamilyName(storageType.getIndexTableName());
		data.setRowKey("/"+columnFamily+"/"+getMetaDbo().getColumnName());
		data.setIndexColumnName(indexColName);
		row.addIndexToPersist(data);
	}
	
	private void copy(byte[] indexColName, byte[] byteVal, int offset) {
		for(int i = 0; i < byteVal.length; i++) {
			indexColName[i+offset] = byteVal[i];
		}
	}
	
}
