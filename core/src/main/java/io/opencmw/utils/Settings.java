package io.opencmw.utils;

/**
 * Interface used by SystemProperties class to automatically initialises public class field values based on:
 * * class default value (lowest priority),
 * * config-file, or
 * * environment variables (e.g. '-Dxxx' VM argument), or
 * * command-line arguments (highest priority) being set.
 * The fields -- if adorned with @see @MetaInfo annotations -- are also used to automatically generate POSIX-inspired
 * command-line interface usage and options declarations.
 *
 *  Beside manually registering the Setting class, you may also want to set the @AutoService(Settings.class) annotation
 *  to the class in order for other utilities to automatically find the Settings class.
 *
 * The field values are set regardless of whether they are static or non-static, and even for 'final' (works only during initialisation)
 * based on the 'class-name'.'field-name' scheme where are they defined while preserving some level of type-safety.
 *
 * E.g. the command-line argument '--MySettings.fieldValue=5' sets the value if 'fieldValue' in class 'MySetting' to 5.
 */
public interface Settings {
}
