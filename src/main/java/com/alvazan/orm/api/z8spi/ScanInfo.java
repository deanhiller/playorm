package com.alvazan.orm.api.z8spi;

import java.io.UnsupportedEncodingException;

import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class ScanInfo {
	private String indexColFamily; 
	private byte[] rowKey;
	private DboTableMeta entityColFamily;
	
	//optional but logging won't work without it
	private DboColumnMeta columnName;

	public static ScanInfo createScanInfo(DboColumnMeta colMeta, String partitionBy, String partitionId) {
		DboTableMeta realColFamily = colMeta.getOwner();
		String columnFamily = colMeta.getIndexTableName();
		String indexRowKey = colMeta.getIndexRowKey(partitionBy, partitionId);
		byte[] rowKey = StandardConverters.convertToBytes(indexRowKey);
		ScanInfo scanInfo = new ScanInfo(realColFamily, colMeta, columnFamily, rowKey);
		return scanInfo;
	}
	
	public ScanInfo(String indexColFamily, DboTableMeta realColFamily, byte[] rowKey2) {
		this.indexColFamily = indexColFamily;
		this.entityColFamily = realColFamily;
		this.rowKey = rowKey2;
	}
	
	public ScanInfo(DboTableMeta realColFamily, DboColumnMeta colName, String indexColFamily,
			byte[] rowKey2) {
		this.entityColFamily = realColFamily;
		this.columnName = colName;
		this.indexColFamily = indexColFamily;
		this.rowKey = rowKey2;
	}
	
	public String getIndexColFamily() {
		return indexColFamily;
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public DboTableMeta getEntityColFamily() {
		return entityColFamily;
	}
	public DboColumnMeta getColumnName() {
		return columnName;
	}

	@Override
	public String toString() {
		return "CF="+indexColFamily+"(for cf="+entityColFamily+") rowKey="+toUTF8(rowKey);
	}

	private String toUTF8(byte[] data) {
		try {
			return new String(data, "UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
