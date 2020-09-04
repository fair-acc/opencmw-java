package io.opencmw.utils;

/**
 * basic ANSI colour code and other escape character definitions @see https://en.wikipedia.org/wiki/ANSI_escape_code
 */
public class AnsiDefs { // NOPMD - nomen est omen
    public static final int MIN_PRINTABLE_CHAR = 32;
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";
}
