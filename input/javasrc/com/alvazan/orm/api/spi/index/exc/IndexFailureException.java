package com.alvazan.orm.api.spi.index.exc;

public class IndexFailureException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public IndexFailureException() {
		super();
	}

	public IndexFailureException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public IndexFailureException(String message, Throwable cause) {
		super(message, cause);
	}

	public IndexFailureException(String message) {
		super(message);
	}

	public IndexFailureException(Throwable cause) {
		super(cause);
	}

	
}
