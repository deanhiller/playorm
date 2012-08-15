package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.spi1.meta.DboColumnMeta;
import com.alvazan.orm.api.spi1.meta.IndexData;
import com.alvazan.orm.api.spi1.meta.InfoForIndex;
import com.alvazan.orm.api.spi1.meta.PartitionTypeInfo;
import com.alvazan.orm.api.spi1.meta.ReflectionUtil;
import com.alvazan.orm.api.spi1.meta.RowToPersist;
import com.alvazan.orm.api.spi1.meta.StorageTypeEnum;

@SuppressWarnings("unchecked")
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
	
	public void setup(Field field2, String colName) {
		this.field = field2;
		this.field.setAccessible(true);
		this.columnName = colName;
	}
	
	protected void removeIndexInfo(InfoForIndex<OWNER> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		
		if(!this.getMetaDbo().isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		else if(fieldToValue == null)
			return;
		
		addIndexRemoves(info, value, byteVal, storageType);
	}
	
	private void addIndexRemoves(InfoForIndex<OWNER> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		RowToPersist row = info.getRow();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		//if we are here, we are indexed, BUT if fieldToValue is null, then it is a brand new entity and not a proxy
		Object originalValue = fieldToValue.get(field);
		if(originalValue == null)
			return;
		else if(originalValue.equals(value))
			return; //previous value is the same, yeah, nothing to do here!!!
			
		byte[] pk = row.getKey();
		byte[] oldIndexedVal = translateValue(originalValue);
		
		List<IndexData> indexList = row.getIndexToRemove();
		
		addToList(info, oldIndexedVal, storageType, pk, indexList);
	}

	private void addToList(InfoForIndex<OWNER> info, byte[] oldIndexedVal, StorageTypeEnum storageType, byte[] pk, List<IndexData> indexList) {
		//original value and current value differ so we need to remove from the index
		List<PartitionTypeInfo> partitionTypes = info.getPartitions();
		for(PartitionTypeInfo part : partitionTypes) {
			//NOTE: Here if we partition by account and security both AND we index both of those to, we only
			//want indexes of /entityCF/account/security/<securityid>
			//           and /entityCF/security/account/<accountid>  
			// It would not be useful at all to have /entityCF/account/account/<accountid> since all the account ids in that index row would be the same!!!!
			if(part.getColMeta() == this)
				continue; //skip indexing if this IS the partition.  
			IndexData data = createAddIndexData(info.getColumnFamily(), oldIndexedVal, storageType, pk, part);
			indexList.add(data);
		}
	}
	
	protected void removingThisEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] pk, StorageTypeEnum storageType) {
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		Object valueInDatabase = fieldToValue.get(field);
		if(valueInDatabase == null)
			return;
		
		byte[] oldIndexedVal = translateValue(valueInDatabase);
		
		addToList(info, oldIndexedVal, storageType, pk, indexRemoves);
	}
	
	protected void addIndexInfo(InfoForIndex<OWNER> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		
		if(!this.getMetaDbo().isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		
		if(!isNeedPersist(entity, value, fieldToValue))
			return;
		
		//original value and current value differ so we need to persist new value
		byte[] pk = row.getKey();
		List<IndexData> indexList = row.getIndexToAdd();
		addToList(info, byteVal, storageType, pk, indexList);
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
			byte[] byteVal, StorageTypeEnum storageType, byte[] pk, PartitionTypeInfo info) {
		IndexData data = new IndexData();
		String colFamily = getMetaDbo().getIndexTableName();
		data.setColumnFamilyName(colFamily);
		String indexRowKey = getMetaDbo().getIndexRowKey(info.getPartitionBy(), info.getPartitionId());
		data.setRowKey(indexRowKey);
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
