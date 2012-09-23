package com.alvazan.ssql.cmdline;

public class InvalidCommand extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidCommand() {
		super();
	}

	public InvalidCommand(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidCommand(String message) {
		super(message);
	}

	public InvalidCommand(Throwable cause) {
		super(cause);
	}
}
