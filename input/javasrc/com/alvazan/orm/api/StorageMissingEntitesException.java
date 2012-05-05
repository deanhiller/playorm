package com.alvazan.orm.api;

import java.util.List;

@SuppressWarnings("rawtypes")
public class StorageMissingEntitesException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private List foundElements;

	public StorageMissingEntitesException() {
		super();
	}
	public StorageMissingEntitesException(List foundElements, String message) {
		super(message);
		this.foundElements = foundElements;
	}
	public List getFoundElements() {
		return foundElements;
	}
	
}
