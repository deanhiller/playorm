package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.exc.ChildWithNoPkException;
import com.alvazan.orm.api.spi1.meta.DboColumnMeta;
import com.alvazan.orm.api.spi1.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.spi1.meta.DboTableMeta;
import com.alvazan.orm.api.spi1.meta.IndexData;
import com.alvazan.orm.api.spi1.meta.InfoForIndex;
import com.alvazan.orm.api.spi1.meta.ReflectionUtil;
import com.alvazan.orm.api.spi1.meta.RowToPersist;
import com.alvazan.orm.api.spi1.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;

public class MetaProxyField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	//ClassMeta Will eventually have the idField that has the converter!!!
	//once it is scanned
	private MetaAbstractClass<PROXY> classMeta;
	private DboColumnToOneMeta metaDbo = new DboColumnToOneMeta();

	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public String toString() {
		return "MetaProxyField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		String columnName = getColumnName();
		Column column = row.getColumn(columnName.getBytes());
		
		if(column == null) {
			column = new Column();
		}
		
		Object proxy = convertIdToProxy(row, column.getValue(), session);
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}
	
	@SuppressWarnings("unchecked")
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Column col = new Column();
		row.getColumns().add(col);
		
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
		byte[] byteVal = classMeta.convertEntityToId(value);
		if(byteVal == null && value != null) { 
			//if value is not null but we get back a byteVal of null, it means the entity has not been
			//initialized with a key yet, BUT this is required to be able to save this object
			String owner = "'"+field.getDeclaringClass().getSimpleName()+"'";
			String child = "'"+field.getType().getSimpleName()+"'";
			String fieldName = "'"+field.getType().getSimpleName()+" "+field.getName()+"'";
			throw new ChildWithNoPkException("The entity you are saving of type="+owner+" has a field="+fieldName
					+" that does not yet have a primary key so you cannot save it.  To correct this\n" +
					"problem, you can either\n"
					+"1. SAVE the "+child+" BEFORE you save the "+owner+" OR\n"
					+"2. Call entityManager.fillInWithKey(Object entity), then SAVE your "+owner+"', then save your "+child+" NOTE that this" +
							"\nmethod #2 is used for when you have a bi-directional relationship where each is a child of the other");
		}
		
		col.setName(columnName.getBytes());
		col.setValue(byteVal);
		
		StorageTypeEnum storageType = getStorageType();
		Object primaryKey = classMeta.fetchId(value);
		addIndexInfo(info, primaryKey, byteVal, storageType);
		removeIndexInfo(info, primaryKey, byteVal, storageType);
	}

	private StorageTypeEnum getStorageType() {
		StorageTypeEnum storageType = classMeta.getIdField().getMetaIdDbo().getStorageType();
		return storageType;
	}
	
	@Override
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] pk) {
		StorageTypeEnum storageType = getStorageType();
		removingThisEntity(info, indexRemoves, pk, storageType);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public byte[] translateValue(Object value) {
		byte[] pk = classMeta.convertEntityToId((PROXY) value);
		if(pk == null && value != null) {
			throw new ChildWithNoPkException("You can't give us an entity with no pk!!!!  We use the pk to search the database index.  Please fix your bug");
		}
		return pk;
	}
	
	public PROXY convertIdToProxy(Row row, byte[] id, NoSqlSession session) {
		Tuple<PROXY> tuple = classMeta.convertIdToProxy(row, id, session, null);
		return tuple.getProxy();
	}
	
	public void setup(DboTableMeta tableMeta, Field field2, String colName, MetaAbstractClass<PROXY> classMeta, boolean isIndexed, boolean isPartitionedBy) {
		DboTableMeta fkToTable = classMeta.getMetaDbo();
		metaDbo.setup(tableMeta, colName, fkToTable, isIndexed, isPartitionedBy);
		super.setup(field2, colName);
		this.classMeta = classMeta;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object unwrapIfNeeded(Object value) {
		PROXY proxy = (PROXY) value;
		return classMeta.fetchId(proxy);
	}

}
