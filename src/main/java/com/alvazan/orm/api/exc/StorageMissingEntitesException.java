package com.alvazan.orm.api.exc;



@SuppressWarnings("rawtypes")
public class StorageMissingEntitesException extends NoSqlOrmException {

	private static final long serialVersionUID = 1L;
	private final Iterable foundElements;

	public StorageMissingEntitesException() {
		super();
		foundElements = null;
	}
	public StorageMissingEntitesException(Iterable foundElements2, String message, RowNotFoundException e) {
		super(message);
		this.foundElements = foundElements2;
	}
	public Iterable getFoundElements() {
		return foundElements;
	}
	
}
