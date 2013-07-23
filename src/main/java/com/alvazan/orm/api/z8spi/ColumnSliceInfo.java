package com.alvazan.orm.api.z8spi;

import java.io.UnsupportedEncodingException;

import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class ColumnSliceInfo {

    private byte[] rowKey;
    private DboTableMeta colFamily;
    private byte[] from;
    private byte[] to;
    private Class columnNameType;

    public ColumnSliceInfo(DboTableMeta realColFamily, byte[] rowKey2, byte[] from2, byte[] to2, Class columnNameType2) {
        this.colFamily = realColFamily;
        this.rowKey = rowKey2;
        this.from = from2;
        this.to = to2;
        this.columnNameType = columnNameType2;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public DboTableMeta getColFamily() {
        return this.colFamily;
    }

    public byte[] getFrom() {
        return this.from;
    }

    public byte[] getTo() {
        return this.to;
    }

    public Class getColumnNameType() {
        return this.columnNameType;
    }

    @Override
    public String toString() {
        String retVal = "CF= " + colFamily.getColumnFamily() + " rowKey= " + toUTF8(rowKey);
        return retVal;
    }

    private String toUTF8(byte[] data) {
        try {
            return new String(data, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
