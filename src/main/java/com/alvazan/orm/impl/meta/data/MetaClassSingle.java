package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.exc.TypeMismatchException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.PartitionTypeInfo;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class MetaClassSingle<T> extends MetaAbstractClass<T> {

	private Class<? extends T> proxyClass;

	/**
	 * This is NOT fieldName to MetaField but columnName to field(columnName can be different than
	 * fieldname)
	 */
	private Map<String,MetaField<T>> columnNameToField = new HashMap<String, MetaField<T>>();
	
	private List<MetaField<T>> indexedColumns = new ArrayList<MetaField<T>>();
	
	private List<MetaField<T>> partitionColumns = new ArrayList<MetaField<T>>();
	
	public void setSharedMetaDbo(DboTableMeta metaDbo) {
		this.metaDbo = metaDbo;
	}
	
	public KeyValue<T> translateFromRow(Row row, NoSqlSession session) {
		byte[] virtual = row.getKey();
		byte[] nonVirtKey = idField.unformVirtRowKey(virtual);
		
		Tuple<T> tuple = convertIdToProxy(row, session, nonVirtKey, null);
		T inst = tuple.getProxy();
		fillInInstance(row, session, inst);
		NoSqlProxy temp = (NoSqlProxy)inst;
		//mark initialized so it doesn't hit the database again.
		temp.__markInitializedAndCacheIndexedValues();
		
		KeyValue<T> keyVal = new KeyValue<T>();
		keyVal.setKey(tuple.getEntityId());
		keyVal.setValue(inst);
		return keyVal;
	}

	/**
	 * @param row
	 * @param session - The session to pass to newly created proxy objects
	 * @param inst The object OR the proxy to be filled in 
	 */
	public void fillInInstance(Row row, NoSqlSession session, T inst) {
		idField.translateFromColumn(row, inst, session);
		if (ttlField != null)
			ttlField.translateFromColumn(row, inst, session);

		for(MetaField<T> field : columnNameToField.values()) {
			field.translateFromColumn(row, inst, session);
		}
	}

	public RowToPersist translateToRow(T entity) {
		RowToPersist row = new RowToPersist();
		Map<Field, Object> fieldToValue = null;
		if(entity instanceof NoSqlProxy) {
			fieldToValue = ((NoSqlProxy) entity).__getOriginalValues();
		}
		
		//We need to get the PartitionTypeInfo's here so we can pass them down
		List<PartitionTypeInfo> partitions = formPartitionTypes(entity);
		InfoForIndex<T> info = new InfoForIndex<T>(entity, row, getColumnFamily(), fieldToValue, partitions);
		
		idField.translateToColumn(info);
		if (ttlField != null)
			ttlField.translateToColumn(info);

		for(MetaField<T> m : columnNameToField.values()) {
			try {
				m.translateToColumn(info);
			} catch(TypeMismatchException e) {
				throw new TypeMismatchException("The entity "+getMetaClass()+" has an incorrect annotation on field="+m.getField().getName()+".  The attribute 'entityType' on the annotation is incorrect for the types you are saving.", e);
			}
		}
		return row;
	}

	private List<PartitionTypeInfo> formPartitionTypes(Object entity) {
		List<PartitionTypeInfo> partInfo = new ArrayList<PartitionTypeInfo>();
		for(MetaField<T> m : partitionColumns) {
			String colName = m.getColumnName();
			Object fieldVal = m.fetchField(entity);
			String value = m.translateToString(fieldVal);
			partInfo.add(new PartitionTypeInfo(colName, value, m));
		}
		
		if(partInfo.size() == 0) {
			//This table is not partitioned so we still need the default null, null partTypeInfo so indexes will process correctly
			partInfo.add(new PartitionTypeInfo(null, null, null));
		}
		
		return partInfo;
	}

	@SuppressWarnings("unchecked")
	public List<IndexData> findIndexRemoves(NoSqlProxy proxy, byte[] rowKey) {
		Map<Field, Object> fieldToValue = proxy.__getOriginalValues();
		List<PartitionTypeInfo> partTypes = formPartitionTypes(proxy);
		InfoForIndex<T> info = new InfoForIndex<T>((T) proxy, null, getColumnFamily(), fieldToValue, partTypes);
		List<IndexData> indexRemoves = new ArrayList<IndexData>();
		idField.removingEntity(info, indexRemoves, rowKey);
		for(MetaField<T> indexed : indexedColumns) {
			indexed.removingEntity(info, indexRemoves, rowKey);
		}
		
		return indexRemoves;
	}
	
	public void addMetaField(MetaField<T> field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		columnNameToField.put(field.getColumnName(), field);
		
		DboColumnMeta metaCol = field.getMetaDbo();
		if(metaCol.isIndexed())
			indexedColumns.add(field);
		
		if(metaCol.isPartitionedByThisColumn())
			partitionColumns.add(field);
	}
	
	public MetaField<T> getMetaFieldByCol(Class clazz, String columnName){
		if(idField != null && idField.getColumnName().equals(columnName))
			return idField;
		return columnNameToField.get(columnName);
	}
	
	public  Collection<MetaField<T>> getMetaFields(){
		return this.columnNameToField.values();
	}
	
	public void setProxyClass(Class<? extends T> proxyClass) {
		this.proxyClass = proxyClass;
	}

	@Override
	public Class<? extends T> getProxyClass(Class<?> type) {
		return proxyClass;
	}

	@Override
	public Tuple<T> convertIdToProxy(Row row, NoSqlSession session, byte[] nonVirtKey, CacheLoadCallback cacheLoadCallback) {
		Tuple<T> t = new Tuple<T>();
		if(nonVirtKey == null)
			return t;
		MetaIdField<T> idField = this.getIdField();
		Converter converter = idField.getConverter();

		Object entityId = converter.convertFromNoSql(nonVirtKey);
		T proxy = idField.convertIdToProxy(session, entityId, cacheLoadCallback, null);
		t.setEntityId(entityId);
		t.setProxy(proxy);
		
		return t;
	}
	
	public List<MetaField<T>> getIndexedColumns() {
		return indexedColumns;
	}

	@Override
	public boolean hasIndexedField(T entity) {
		if(indexedColumns.size() > 0 || idField.getMetaDbo().isIndexed())
			return true;
		return false;
	}

	@Override
	public boolean isPartitioned() {
		return partitionColumns.size() > 0;
	}

}
