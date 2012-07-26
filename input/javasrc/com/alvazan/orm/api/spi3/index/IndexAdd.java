package com.alvazan.orm.api.spi3.index;

import java.util.Map;


public class IndexAdd extends IndexRemoveImpl {

	private Map<String, Object> item;

	public void setItem(Map<String, Object> item) {
		this.item = item;
	}

	public Map<String, Object> getItem() {
		return item;
	}

}
