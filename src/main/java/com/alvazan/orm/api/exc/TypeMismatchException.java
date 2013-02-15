package com.alvazan.orm.api.exc;


public class TypeMismatchException extends NoSqlOrmException {

	private static final long serialVersionUID = 1L;

	public TypeMismatchException() {
		super();
	}

	public TypeMismatchException(String message, Throwable cause) {
		super(message, cause);
	}

	public TypeMismatchException(String message) {
		super(message);
	}

	public TypeMismatchException(Throwable cause) {
		super(cause);
	}

	
}
