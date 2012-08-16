package com.alvazan.orm.api.exc;

public class NoSqlOrmException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public NoSqlOrmException() {
		super();
	}

	public NoSqlOrmException(String arg0, Throwable arg1, boolean arg2,
			boolean arg3) {
//		super(arg0, arg1, arg2, arg3);
	}

	public NoSqlOrmException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public NoSqlOrmException(String arg0) {
		super(arg0);
	}

	public NoSqlOrmException(Throwable arg0) {
		super(arg0);
	}
	
}
