package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Map;

import com.alvazan.orm.api.base.exc.ChildWithNoPkException;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.StorageTypeEnum;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;

public class MetaProxyField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	//ClassMeta Will eventually have the idField that has the converter!!!
	//once it is scanned
	private MetaClass<PROXY> classMeta;
	
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
		
		Object proxy = convertIdToProxy(column.getValue(), session);
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}
	
	@SuppressWarnings("unchecked")
	public void translateToColumn(OWNER entity, RowToPersist row, String columnFamilyName, Map<Field, Object> fieldToValue) {
		Column col = new Column();
		row.getColumns().add(col);
		
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
		byte[] byteVal = classMeta.convertProxyToId(value);
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
		
		StorageTypeEnum storageType = classMeta.getIdField().getMetaDbo().getStorageType();
		Object primaryKey = classMeta.fetchId(value);
		addIndexInfo(entity, row, columnFamilyName, primaryKey, byteVal, storageType, fieldToValue);
		removeIndexInfo(entity, row, columnFamilyName, primaryKey, byteVal, storageType, fieldToValue);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public byte[] translateValue(Object value) {
		byte[] pk = classMeta.convertProxyToId((PROXY) value);
		if(pk == null && value != null) {
			throw new ChildWithNoPkException("You can't give us an entity with no pk!!!!  We use the pk to search the database index.  Please fix your bug");
		}
		return pk;
	}
	
	public PROXY convertIdToProxy(byte[] id, NoSqlSession session) {
		return classMeta.convertIdToProxy(id, session, null).getProxy();
	}
	
	public void setup(Field field2, String colName, MetaClass<PROXY> classMeta, String indexPrefix) {
		DboTableMeta fkToTable = classMeta.getMetaDbo();
		
		super.setup(field2, colName, fkToTable, null, false, indexPrefix);
		this.classMeta = classMeta;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object unwrapIfNeeded(Object value) {
		PROXY proxy = (PROXY) value;
		return classMeta.fetchId(proxy);
	}

}
