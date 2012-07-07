package com.alvazan.orm.api.spi.index;

import java.util.Map;


public class IndexAdd implements IndexRemove {

	private Map<String, String> item;

	public void setItem(Map<String, String> item) {
		this.item = item;
	}

	public Map<String, String> getItem() {
		return item;
	}

	@Override
	public String getId() {
		return item.get("id");
	}
}
