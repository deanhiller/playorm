package com.alvazan.orm.api.base.anno;

import com.alvazan.orm.api.spi.layer2.Converter;

public class NoConversion implements Converter {
	@Override
	public byte[] convertToNoSql(Object value) {
		return null;
	}
	@Override
	public Object convertFromNoSql(byte[] value) {
		return null;
	}
	@Override
	public String convertToIndexFormat(Object value) {
		throw new UnsupportedOperationException();
	}
}
