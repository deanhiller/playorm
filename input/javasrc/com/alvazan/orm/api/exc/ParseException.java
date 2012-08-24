package com.alvazan.orm.api.exc;

public class ParseException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ParseException() {
	}

	public ParseException(String message) {
		super(message);
	}

	public ParseException(Throwable cause) {
		super(cause);
	}

	public ParseException(String message, Throwable cause) {
		super(message, cause);
	}


}
