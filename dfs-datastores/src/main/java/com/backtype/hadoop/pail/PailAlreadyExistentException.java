package com.backtype.hadoop.pail;

/**
 * Exception to throw when trying to create a pail but pail.meta is existent in the file system layout.
 */
public class PailAlreadyExistentException extends IllegalArgumentException {

    public PailAlreadyExistentException(String s) {
        super(s);
    }
}
