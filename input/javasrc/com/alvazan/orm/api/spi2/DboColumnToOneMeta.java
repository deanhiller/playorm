package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.spi3.db.Column;

@NoSqlDiscriminatorColumn(value="fk")
public class DboColumnToOneMeta extends DboColumnMeta {

	private String indexPrefix;
	
	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	@ManyToOne
	private DboTableMeta fkToColumnFamily;

	@Override
	public boolean isIndexed() {
		if(indexPrefix == null)
			return false;
		return true;
	}

	public void setup(String colName, DboTableMeta fkToTable,
			String indexPrefix2) {
		this.columnName = colName;
		this.fkToColumnFamily = fkToTable;
		this.indexPrefix = indexPrefix2;
	}

	public String getIndexPrefix() {
		return indexPrefix;
	}

	public DboTableMeta getFkToColumnFamily() {
		return fkToColumnFamily;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getClassType() {
		return fkToColumnFamily.getIdColumnMeta().getClassType();
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return fkToColumnFamily.getIdColumnMeta().getStorageType();
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Column col = new Column();
		row.getColumns().add(col);
	
		TypedColumn column = entity.getColumn(columnName);
		
		byte[] byteVal = convertToStorage2(column.getValue());
		col.setName(columnName.getBytes());
		col.setValue(byteVal);
		Object primaryKey = column.getValue();
		StorageTypeEnum storageType = getStorageType();
		addIndexInfo(info, primaryKey, byteVal, storageType);
		removeIndexInfo(info, primaryKey, byteVal, storageType);		
	}
	
}
