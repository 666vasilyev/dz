package org.parsers.Exceptions;

public class ParsingException extends Exception {
    public ParsingException(
            String url
    ) {
        super(String.format("Exception while parsing URL: %s", url));
    }

}
