package com.alvazan.orm.api.spi.index.exc;

public class IndexNotYetExistException extends IndexFailureException {

	private static final long serialVersionUID = 1L;

	public IndexNotYetExistException() {
		super();
	}

	public IndexNotYetExistException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public IndexNotYetExistException(String message, Throwable cause) {
		super(message, cause);
	}

	public IndexNotYetExistException(String message) {
		super(message);
	}

	public IndexNotYetExistException(Throwable cause) {
		super(cause);
	}
	
}
