package com.alvazan.orm.api.z5api;

import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class RowKey {

	private DboColumnIdMeta idMeta;
	private byte[] rowKey;

	public RowKey(ViewInfo info, IndexColumn value) {
		idMeta = info.getTableMeta().getIdColumnMeta();
		rowKey = value.getPrimaryKey();
	}

	public Object getKey() {
		return idMeta.convertFromStorage2(rowKey);
	}
	public String getKeyAsString() {
		Object obj = getKey();
		return idMeta.convertTypeToString(obj);
	}
	
	public byte[] getRawKey() {
		return rowKey;
	}
}
