package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.base.ToOneProvider;
import com.alvazan.orm.api.exc.ChildWithNoPkException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.ToOneProviderProxy;

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
		byte[] colBytes = StandardConverters.convertToBytes(columnName);
		Column column = row.getColumn(colBytes);
		if(column == null) {
			column = new Column();
		}
		Object proxy;
		if(field.getType().equals(ToOneProvider.class)) {
			proxy = translateFromToComposite(row, session);
			if (proxy == null)
				proxy = translateFromToProxy(row, column.getValue(), session);
		}
		else {
			proxy = convertIdToProxyComposite(row, session);
			if (proxy == null)
				proxy = convertIdToProxy(row, column.getValue(), session);
		}
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}
	
	private Object translateFromToProxy(Row row, byte[] value,
			NoSqlSession session) {
		ToOneProvider<PROXY> toOne = new ToOneProviderProxy(classMeta, value, session);
		return toOne;
	}

	private Object translateFromToComposite(Row row, NoSqlSession session) {
		byte[] bytes = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(bytes);
		if (columns != null && !columns.isEmpty()) {
			Column column = columns.iterator().next();
			byte[] fullName = column.getName();
			//strip off the prefix to get the foreign key
			int pkLen = fullName.length-bytes.length;
			byte[] fk = new byte[pkLen];
			for(int i = bytes.length; i < fullName.length; i++) {
				fk[i-bytes.length] =  fullName[i];
			}
			ToOneProvider<PROXY> toOne = new ToOneProviderProxy(classMeta, fk, session);
			return toOne;
		}
		else
			return null;
	}

	private Object convertIdToProxyComposite(Row row, NoSqlSession session) {
		byte[] bytes = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(bytes);
		if (columns != null && !columns.isEmpty()) {
			Column column = columns.iterator().next();
			byte[] fullName = column.getName();
			//strip off the prefix to get the foreign key
			int pkLen = fullName.length-bytes.length;
			byte[] fk = new byte[pkLen];
			for(int i = bytes.length; i < fullName.length; i++) {
				fk[i-bytes.length] =  fullName[i];
			}
			Tuple<PROXY> tuple = classMeta.convertIdToProxy(row, session, fk, null);
			return tuple.getProxy();
		}
		else 
			return null;
	}

	@SuppressWarnings("unchecked")
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		Column col = new Column();
		row.getColumns().add(col);

		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		
		if(value instanceof ToOneProvider) {
			value = (PROXY) ((ToOneProvider)value).get();
		}

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

		byte[] colBytes = StandardConverters.convertToBytes(columnName);
		if(byteVal != null) {
			byte[] name = new byte[colBytes.length + byteVal.length];
			for(int i = 0; i < name.length; i++) {
				if(i < colBytes.length)
					name[i] = colBytes[i];
				else
					name[i] = byteVal[i-colBytes.length];
			}
			col.setName(name);
			StorageTypeEnum storageType = getStorageType();
			Object primaryKey = classMeta.fetchId(value);
			addIndexInfo(info, primaryKey, byteVal, storageType);
			removeIndexInfo(info, primaryKey, byteVal, storageType);
		}
		else {
			col.setName(colBytes);
			col.setValue(byteVal);
			StorageTypeEnum storageType = getStorageType();
			Object primaryKey = classMeta.fetchId(value);
			addIndexInfo(info, primaryKey, byteVal, storageType);
			removeIndexInfo(info, primaryKey, byteVal, storageType);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object fetchField(Object entity) {
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		return value;
		//throw new UnsupportedOperationException("only used for partitioning and multivalue column can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String translateToString(Object fieldsValue) {
		Object id = classMeta.fetchId((PROXY) fieldsValue);
		return classMeta.getIdField().getConverter().convertTypeToString(id);
	}
	
	private StorageTypeEnum getStorageType() {
		StorageTypeEnum storageType = classMeta.getIdField().getMetaIdDbo().getStorageType();
		return storageType;
	}
	
	@Override
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] pk) {
		removingThisEntity(info, indexRemoves, pk);
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
	
	public PROXY convertIdToProxy(Row row, byte[] nonVirtFk, NoSqlSession session) {
		Tuple<PROXY> tuple = classMeta.convertIdToProxy(row, session, nonVirtFk, null);
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
