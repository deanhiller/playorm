package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

@NoSqlDiscriminatorColumn(value="generic")
public class DboColumnCommonMeta extends DboColumnMeta {

	private String columnValueType;
	private String indexPrefix;

	@SuppressWarnings("rawtypes")
	public void setup(String colName, Class valuesType, String indexPrefix) {
		Class newType = translateType(valuesType);
		this.columnName = colName;
		this.columnValueType = newType.getName();
		this.indexPrefix = indexPrefix;
	}
	
	public String getIndexPrefix() {
		return indexPrefix;
	}

	@Override
	public boolean isIndexed() {
		if(indexPrefix == null)
			return false;
		return true;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getClassType() {
		return classForName(columnValueType);
	}

	@SuppressWarnings("rawtypes")
	public StorageTypeEnum getStorageType() {
		Class fieldType = getClassType();
		return getStorageType(fieldType);
	}



}
