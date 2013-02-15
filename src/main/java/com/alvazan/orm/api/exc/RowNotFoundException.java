package com.alvazan.orm.api.exc;


public class RowNotFoundException extends NoSqlOrmException {

	private static final long serialVersionUID = 1L;

	public RowNotFoundException() {
		super();
	}

	public RowNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public RowNotFoundException(String message) {
		super(message);
	}

	public RowNotFoundException(Throwable cause) {
		super(cause);
	}

}
