package indi.yangshenhui.hadoop.dao.exception;

/**
 * 
 * @author yangshenhui
 */
public class HdfsDaoException extends Exception {
	private static final long serialVersionUID = 1L;

	public HdfsDaoException() {
	}

	public HdfsDaoException(String message) {
		super(message);
	}

	public HdfsDaoException(String message, Throwable throwable) {
		super(message, throwable);
	}

	public HdfsDaoException(Throwable throwable) {
		super(throwable);
	}

}