package com.alvazan.orm.api.spi.index.exc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class IndexErrorInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private Exception cause;
	private List<Map<String, String>> itemsThatFailed;
	private List<String> idsThatFailed;
	
	public Exception getCause() {
		return cause;
	}
	public void setCause(Exception cause) {
		this.cause = cause;
	}
	public List<Map<String, String>> getItemsThatFailed() {
		return itemsThatFailed;
	}
	public void setItemsThatFailed(List<Map<String, String>> items) {
		this.itemsThatFailed = items;
	}
	public void setIdsThatFailed(List<String> ids) {
		this.idsThatFailed = ids;
	}
	public List<String> getIdsThatFailed() {
		return idsThatFailed;
	}
}
