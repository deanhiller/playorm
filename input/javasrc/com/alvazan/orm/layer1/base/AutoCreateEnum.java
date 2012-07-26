package com.alvazan.orm.layer1.base;

import java.util.HashMap;
import java.util.Map;

public enum AutoCreateEnum {
	UPDATE("update"), FAIL_IF_NOT_VALID("validate"), CREATE_ONLY("create");

	private static Map<String, AutoCreateEnum> mapping = new HashMap<String, AutoCreateEnum>();
	static {
		for(AutoCreateEnum a : AutoCreateEnum.values()) {
			mapping.put(a.getValue(), a);
		}
	}
	
	private String value;
	private AutoCreateEnum(String val) {
		this.value = val;
	}
	
	public String getValue() {
		return value;
	}
	
	public static AutoCreateEnum translate(String val) {
		return mapping.get(val);
	}
}
