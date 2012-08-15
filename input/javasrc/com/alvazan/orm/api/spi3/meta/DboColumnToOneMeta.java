package com.alvazan.orm.api.spi3.meta;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.spi3.TypedColumn;
import com.alvazan.orm.api.spi3.TypedRow;
import com.alvazan.orm.api.spi3.meta.conv.StandardConverters;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.Row;

@SuppressWarnings("rawtypes")
@NoSqlDiscriminatorColumn(value="fk")
public class DboColumnToOneMeta extends DboColumnMeta {
	
	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	@NoSqlManyToOne
	private DboTableMeta fkToColumnFamily;

	private boolean isPartitionedByThisColumn;

	public void setup(DboTableMeta owner, String colName, DboTableMeta fkToTable,
			boolean isIndexed, boolean isPartitionedBy) {
		super.setup(owner, colName, isIndexed);
		this.fkToColumnFamily = fkToTable;
		this.isPartitionedByThisColumn = isPartitionedBy;
	}

	public boolean isPartitionedByThisColumn() {
		return isPartitionedByThisColumn;
	}

	@Override
	public String getIndexTableName() {
		return getStorageType().getIndexTableName();
	}

	public DboTableMeta getFkToColumnFamily() {
		return fkToColumnFamily;
	}

	@Override
	public Class getClassType() {
		return fkToColumnFamily.getIdColumnMeta().getClassType();
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return fkToColumnFamily.getIdColumnMeta().getStorageType();
	}

	public void translateFromColumn(Row row, TypedRow entity) {
		Column column = row.getColumn(getColumnNameAsBytes());
		if(column == null) {
			return;
		}

		Object value = convertFromStorage2(column.getValue());
		TypedColumn typedCol = new TypedColumn();
		typedCol.setName(columnName);
		typedCol.setValue(value);
		typedCol.setTimestamp(column.getTimestamp());
		entity.addColumn(typedCol);
	}
	
	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Column col = new Column();
		row.getColumns().add(col);
	
		TypedColumn column = entity.getColumn(columnName);
		
		byte[] byteVal = convertToStorage2(column.getValue());
		byte[] colBytes = StandardConverters.convertToBytes(columnName);
		col.setName(colBytes);
		col.setValue(byteVal);
		Object primaryKey = column.getValue();
		StorageTypeEnum storageType = getStorageType();
		addIndexInfo(info, primaryKey, byteVal, storageType);
		removeIndexInfo(info, primaryKey, byteVal, storageType);		
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		TypedColumn typedCol = row.getColumn(columnName);
		Object value = typedCol.getValue();
		return convertTypeToString(value);
	}
	
}
