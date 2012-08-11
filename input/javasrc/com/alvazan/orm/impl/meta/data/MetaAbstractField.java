package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.StorageTypeEnum;

public abstract class MetaAbstractField<OWNER> implements MetaField<OWNER> {

	protected Field field;
	protected String columnName;
	
	public Field getField() {
		return field;
	}
	
	public abstract DboColumnMeta getMetaDbo();
	
	public String getColumnName() {
		return columnName;
	}

	public final String getFieldName() {
		return field.getName();
	}
	
	@SuppressWarnings("rawtypes")
	public void setup(Field field2, String colName) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
	}
	
	protected void removeIndexInfo(InfoForIndex<OWNER> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		RowToPersist row = info.getRow();
		String columnFamily = info.getColumnFamily();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		
		if(!this.getMetaDbo().isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		else if(fieldToValue == null)
			return;
		
		addIndexRemoves(row, columnFamily, value, byteVal, storageType, fieldToValue);
	}
	
	private void addIndexRemoves(RowToPersist row, String columnFamily,
			Object value, byte[] byteVal, StorageTypeEnum storageType, Map<Field, Object> fieldToValue) {
		//if we are here, we are indexed, BUT if fieldToValue is null, then it is a brand new entity and not a proxy
		Object originalValue = fieldToValue.get(field);
		if(originalValue == null)
			return;
		else if(originalValue.equals(value))
			return; //previous value is the same, yeah, nothing to do here!!!
			
		byte[] oldIndexedVal = translateValue(originalValue);
		byte[] pk = row.getKey();
		//original value and current value differ so we need to remove from the index
		IndexData data = createAddIndexData(columnFamily, oldIndexedVal, storageType, pk );
		row.addIndexToRemove(data);
	}
	
	protected void removingThisEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] pk, StorageTypeEnum storageType) {
		String columnFamily = info.getColumnFamily();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		Object valueInDatabase = fieldToValue.get(field);
		if(valueInDatabase == null)
			return;
		
		byte[] oldIndexedVal = translateValue(valueInDatabase);
		IndexData data = createAddIndexData(columnFamily, oldIndexedVal, storageType, pk);
		indexRemoves.add(data);
	}
	
	protected void addIndexInfo(InfoForIndex<OWNER> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		String columnFamily = info.getColumnFamily();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		
		if(!this.getMetaDbo().isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		
		if(!isNeedPersist(entity, value, fieldToValue))
			return;
		
		//original value and current value differ so we need to persist new value
		byte[] pk = row.getKey();
		IndexData data = createAddIndexData(columnFamily, byteVal, storageType, pk);
		row.addIndexToPersist(data);
	}

	private boolean isNeedPersist(OWNER entity, Object value, Map<Field, Object> fieldToValue) {
		if(!(entity instanceof NoSqlProxy))
			return true;
		Object originalValue = fieldToValue.get(field);
		if(value == null) //new value is null so nothing to persist 
			return false;
		else if(value.equals(originalValue))
			return false; //previous value is the same, yeah, nothing to do here!!!
		
		return true;
	}
	private IndexData createAddIndexData(String columnFamily,
			byte[] byteVal, StorageTypeEnum storageType, byte[] pk) {
		IndexData data = new IndexData();
		data.setColumnFamilyName(storageType.getIndexTableName());
		data.setRowKey("/"+columnFamily+"/"+getMetaDbo().getColumnName());
		data.getIndexColumn().setIndexedValue(byteVal);
		data.getIndexColumn().setPrimaryKey(pk);
		return data;
	}

	@Override
	public Object getFieldRawValue(OWNER entity) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		return unwrapIfNeeded(value);
	}

	protected abstract Object unwrapIfNeeded(Object value);
}
