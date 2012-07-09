package com.alvazan.orm.api.spi.index;

import java.util.Map;


public class IndexAdd extends IndexRemoveImpl {

	private Map<String, String> item;

	public void setItem(Map<String, String> item) {
		this.item = item;
	}

	public Map<String, String> getItem() {
		return item;
	}

}
