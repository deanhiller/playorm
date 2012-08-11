package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

@NoSqlDiscriminatorColumn(value="listOfFk")
public class DboColumnToManyMeta extends DboColumnMeta {

	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	@ManyToOne
	private DboTableMeta fkToColumnFamily;

	@Override
	public boolean isIndexed() {
		return false;
	}

	public void setup(String colName, DboTableMeta fkToTable) {
		this.columnName = colName;
		this.fkToColumnFamily = fkToTable;
	}

	public DboTableMeta getFkToColumnFamily() {
		return fkToColumnFamily;
	}

	@Override
	public String getIndexPrefix() {
		throw new UnsupportedOperationException("bug, this should not be called.  it's not supported");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getClassType() {
		throw new UnsupportedOperationException("Need to figure out how to convert Class to a type of Array Class");
//		Class typeInTheArray = fkToColumnFamily.getIdColumnMeta().getClassType();
	}

	@Override
	public StorageTypeEnum getStorageType() {
		throw new UnsupportedOperationException("Need to figure out how to convert Class to a type of Array Class");
//		StorageTypeEnum typeInTheArray = fkToColumnFamily.getIdColumnMeta().getStorageType();
	}

}
