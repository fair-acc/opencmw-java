package io.opencmw;

/**
 * Basic filter interface description
 *
 * @author rstein
 *  N.B. while 'toString()', 'hashCode()' and 'equals()' is ubiquously defined via the Java 'Object' class, these definition are kept for symmetry with the C++ implementation
 */
public interface Filter {
    /**
     * reinitialises the filter to safe default values
     */
    void clear();

    /**
     * @param other filter this filter should copy its data to
     */
    void copyTo(Filter other);

    @Override
    boolean equals(Object other);

    /**
     * @return unique query key value that should be unique for the given application. This is being used for context matching.
     */
    String getKey();

    /**
     * @return string representation of actual filter state (should be less verbose than @see #toString()). This is being used for context matching.
     */
    String getValue();

    /**
     * @param ctxString the new string representation of the filter. Must match @see #getValue().
     * @return filter that has been initialised with it's string representation. This is being used for context matching.
     */
    Filter get(final String ctxString);

    boolean matches(final Filter other);

    //boolean matches(final String valueOther);

    @Override
    int hashCode();

    /**
     * @return filter description including internal state (if any).
     */
    @Override
    String toString();
}
