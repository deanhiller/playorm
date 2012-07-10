package com.alvazan.orm.api.spi.layer2;

public class ParamInfo {

	private String parameter;
	private TypeInfo typeInfo;

	public ParamInfo(String parameter, TypeInfo typeInfo) {
		this.parameter = parameter;
		this.typeInfo = typeInfo;
	}

	public String getParameter() {
		return parameter;
	}

	public TypeInfo getTypeInfo() {
		return typeInfo;
	}

}
