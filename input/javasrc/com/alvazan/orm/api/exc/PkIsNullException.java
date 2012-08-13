package com.alvazan.orm.api.exc;


public class PkIsNullException extends NoSqlOrmException {

	private static final long serialVersionUID = 1L;

	public PkIsNullException() {
		super();
	}

	public PkIsNullException(String arg0, Throwable arg1, boolean arg2,
			boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	public PkIsNullException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public PkIsNullException(String arg0) {
		super(arg0);
	}

	public PkIsNullException(Throwable arg0) {
		super(arg0);
	}
	
}
