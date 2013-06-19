package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.base.CursorToManyImpl;
import com.alvazan.orm.api.exc.ChildWithNoPkException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.CursorProxy;

public final class MetaCursorField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private MetaAbstractClass<OWNER> ownerMeta;
	private MetaAbstractClass<PROXY> classMeta;
	private DboColumnToManyMeta metaDbo = new DboColumnToManyMeta();
	
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		String indexColFamily = getMetaDbo().getIndexTableName();
		String rowKey = formRowKey(row.getKey());
		
		byte[] key = StandardConverters.convertToBytes(rowKey);
		ScanInfo info = new ScanInfo(ownerMeta.getMetaDbo(), getMetaDbo(), indexColFamily, key);
		int batchSize = 200;
		AbstractCursor<IndexColumn> indexCursor = session.scanIndex(info , null, null, batchSize);
		
		CursorProxy<PROXY> cursor = new CursorProxy<PROXY>(entity, session, indexCursor, classMeta, batchSize);
		ReflectionUtil.putFieldValue(entity, field, cursor);
	}

	private String formRowKey(byte[] byteKey) {
		Converter converter = ownerMeta.getIdField().getConverter();
		Object objKey = converter.convertFromNoSql(byteKey);
		String keyAsStr = converter.convertTypeToString(objKey);
		
		//We ALWAYS ignore partition in this case since this index row is ALWAYS tied to this row period...moving it all
		//would be a pain and be useless...
		String rowKey = getMetaDbo().getIndexRowKey(null, null)+"/"+keyAsStr;
		return rowKey;
	}

	@Override
	public Object fetchField(Object entity) {
		throw new UnsupportedOperationException("only used for partitioning and multivalue column can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}

	@Override
	public String translateToString(Object fieldsValue) {
		throw new UnsupportedOperationException("only used for partitioning and multievalue column can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		Object cursor = ReflectionUtil.fetchFieldValue(entity, field);
		if(cursor == null)
			return; //just ignore it then since user is not modifying what is in the cursor
		
		if(!(cursor instanceof CursorToMany))
			throw new IllegalArgumentException("cursor must be of type CursorToMany");
		
		//NOTE: IF instance of proxy, we can just add AND remove modified items only!!!
		if(cursor instanceof CursorProxy) {
			addRemoveItems(info, (CursorProxy<PROXY>) cursor);
			return;
		}
		
		//If it is not our proxy and is brand new cursor, we need to add all of them to the index...
		CursorToMany<PROXY> c = (CursorToMany<PROXY>) cursor;
		c.beforeFirst();
		while(c.next()) {
			PROXY current = c.getCurrent();
			translateToColumn(info, current);
		}
		((CursorToManyImpl<PROXY>)c).getElementsToAdd().clear();
	}

	private void addRemoveItems(InfoForIndex<OWNER> info, CursorProxy<PROXY> cursor) {
		List<PROXY> elementsToAdd = cursor.getElementsToAdd();
		List<PROXY> elementsToRemove = cursor.getElementsToRemove();
		
		for(PROXY p : elementsToAdd) {
			translateToColumn(info, p);
		}
		elementsToAdd.clear();
		
		for(PROXY p : elementsToRemove) {
			RowToPersist row = info.getRow();
			IndexData data = fetchIndexData(info, p);
			row.addIndexToRemove(data);
		}
		elementsToRemove.clear();
	}

	private void translateToColumn(InfoForIndex<OWNER> info, PROXY value) {
		RowToPersist row = info.getRow();
		IndexData data = fetchIndexData(info, value);
		row.addIndexToPersist(data);
	}

	private IndexData fetchIndexData(InfoForIndex<OWNER> info, PROXY value) {
		RowToPersist row = info.getRow();
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
		
		byte[] key = info.getRow().getKey();
		
		IndexData data = new IndexData();
		String colFamily = getMetaDbo().getIndexTableName();
		String rowKey = formRowKey(row.getKey());
		
		data.setColumnFamilyName(colFamily);
		data.setRowKey(rowKey);
		IndexColumn indCol = data.getIndexColumn();
		indCol.setIndexedValue(byteVal);
		indCol.setPrimaryKey(byteVal);
		return data;
	}

	public void setup(DboTableMeta tableMeta, Field field, String colName, 
			MetaAbstractClass<OWNER> ownerMeta, MetaAbstractClass<PROXY> classMeta) {
		DboTableMeta fkToTable = classMeta.getMetaDbo();
		metaDbo.setup(tableMeta, colName, fkToTable, true);
		super.setup(field, colName);
		this.classMeta = classMeta;
		this.ownerMeta = ownerMeta;
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
