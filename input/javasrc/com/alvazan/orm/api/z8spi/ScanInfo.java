package com.alvazan.orm.api.z8spi;

import java.io.UnsupportedEncodingException;

import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class ScanInfo {
	private String indexColFamily; 
	private byte[] rowKey;
	//optional but logging won't work without it
	private String entityColFamily;
	//optional but logging won't work without it
	private String columnName;

	public static ScanInfo createScanInfo(DboColumnMeta colMeta, String partitionBy, String partitionId) {
		String realColFamily = colMeta.getOwner().getColumnFamily();
		String colName = colMeta.getColumnName();
		String columnFamily = colMeta.getIndexTableName();
		String indexRowKey = colMeta.getIndexRowKey(partitionBy, partitionId);
		byte[] rowKey = StandardConverters.convertToBytes(indexRowKey);
		ScanInfo scanInfo = new ScanInfo(realColFamily, colName, columnFamily, rowKey);
		return scanInfo;
	}
	
	public ScanInfo(String indexColFamily, byte[] rowKey2) {
		this.indexColFamily = indexColFamily;
		this.rowKey = rowKey2;
	}
	
	public ScanInfo(String realColFamily, String colName, String indexColFamily,
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
	public String getEntityColFamily() {
		return entityColFamily;
	}
	public String getColumnName() {
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
