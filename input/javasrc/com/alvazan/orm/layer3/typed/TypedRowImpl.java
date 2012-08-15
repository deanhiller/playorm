package com.alvazan.orm.layer3.typed;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi3.TypedColumn;
import com.alvazan.orm.api.spi3.TypedRow;

public class TypedRowImpl<T> extends TypedRow<T> {
	private Map<String, TypedColumn> originalValues = new HashMap<String, TypedColumn>();

	public Map<String, TypedColumn> getOriginalValues() {
		return originalValues;
	}

	public void setOriginalValues(Map<String, TypedColumn> originalValues) {
		this.originalValues = originalValues;
	}
	
}