package com.alvazan.orm.api.base;

public class TooManyResultException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public TooManyResultException() {
		super();
	}

	public TooManyResultException(String message, Throwable cause) {
		super(message, cause);
	}

	public TooManyResultException(String message) {
		super(message);
	}

	public TooManyResultException(Throwable cause) {
		super(cause);
	}
	
}
