package com.alvazan.orm.api.spi3.meta;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.spi3.TypedRow;
import com.alvazan.orm.api.spi9.db.Row;

@SuppressWarnings({ "unchecked", "rawtypes" })
@NoSqlDiscriminatorColumn(value="id")
public class DboColumnIdMeta extends DboColumnMeta {

	protected String columnValueType;

	@Override
	public String getIndexTableName() {
		return getStorageType().getIndexTableName();
	}

	public Class getClassType() {
		return classForName(columnValueType);
	}

	public StorageTypeEnum getStorageType() {
		Class fieldType = getClassType();
		return getStorageType(fieldType);
	}
	
	public void setup(DboTableMeta owner, String colName, Class valuesType, boolean isIndexed) {
		if(owner.getColumnFamily() == null)
			throw new IllegalArgumentException("The owner passed in must have a non-null column family name");
		else if(colName == null)
			throw new IllegalStateException("colName parameter must not be null");
		this.owner = owner;
		this.columnName = colName;
		//NOTE: We don't call super.setup here BECAUSE super.setup calls owner.addColumn and this is the row key
		//and so we call owner.setRowKeyMeta here instead
		owner.setRowKeyMeta(this);
		id = owner.getColumnFamily()+":"+columnName;
		
		Class newType = translateType(valuesType);
		this.columnValueType = newType.getName();
		this.isIndexed = isIndexed;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();

		Object id = entity.getRowKey();
		if(id == null)
			throw new IllegalArgumentException("rowKey cannot be null");
		
		byte[] byteVal = convertToStorage2(id);
		row.setKey(byteVal);
		
		StorageTypeEnum storageType = getStorageType();
		addIndexInfo(info, id, byteVal, storageType);
		//NOTICE: there is no call to remove because if an id is changed, it is a new entity and we only need to add index not remove since
		//the old entity was not deleted during this save....we remove from index on remove of old entity only
	}

	public void translateFromColumn(Row row, TypedRow entity) {
		byte[] rowKey = row.getKey();
		Object pk = convertFromStorage2(rowKey);
		entity.setRowKey(pk);
	}

	@Override
	public boolean isPartitionedByThisColumn() {
		return false;
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		throw new UnsupportedOperationException("only used by partitioning and can't partition a primary key column....that is kind of useless");
	}
}
