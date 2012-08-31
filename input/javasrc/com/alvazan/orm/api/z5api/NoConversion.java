package com.alvazan.orm.api.z5api;

import com.alvazan.orm.api.z8spi.conv.Converter;

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
	public Object convertStringToType(String value) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String convertTypeToString(Object dbValue) {
		// TODO Auto-generated method stub
		return null;
	}
}
