package io.github.interestinglab.waterdrop.config.parser;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigParseOptions;
import io.github.interestinglab.waterdrop.config.impl.Parseable;

import java.io.File;
import java.io.Reader;

/**
 * Factory for creating {@link
 * ConfigDocument} instances.
 */
public final class ConfigDocumentFactory {

    /**
     * Parses a Reader into a ConfigDocument instance.
     *
     * @param reader
     *       the reader to parse
     * @param options
     *       parse options to control how the reader is interpreted
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static ConfigDocument parseReader(Reader reader, ConfigParseOptions options) {
        return Parseable.newReader(reader, options).parseConfigDocument();
    }

    /**
     * Parses a reader into a Config instance as with
     * {@link #parseReader(Reader,ConfigParseOptions)} but always uses the
     * default parse options.
     *
     * @param reader
     *       the reader to parse
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static ConfigDocument parseReader(Reader reader) {
        return parseReader(reader, ConfigParseOptions.defaults());
    }

    /**
     * Parses a file into a ConfigDocument instance.
     *
     * @param file
     *       the file to parse
     * @param options
     *       parse options to control how the file is interpreted
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static ConfigDocument parseFile(File file, ConfigParseOptions options) {
        return Parseable.newFile(file, options).parseConfigDocument();
    }

    /**
     * Parses a file into a ConfigDocument instance as with
     * {@link #parseFile(File,ConfigParseOptions)} but always uses the
     * default parse options.
     *
     * @param file
     *       the file to parse
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static ConfigDocument parseFile(File file) {
        return parseFile(file, ConfigParseOptions.defaults());
    }

    /**
     * Parses a string which should be valid HOCON or JSON.
     *
     * @param s string to parse
     * @param options parse options
     * @return the parsed configuration
     */
    public static ConfigDocument parseString(String s, ConfigParseOptions options) {
        return Parseable.newString(s, options).parseConfigDocument();
    }

    /**
     * Parses a string (which should be valid HOCON or JSON). Uses the
     * default parse options.
     *
     * @param s string to parse
     * @return the parsed configuration
     */
    public static ConfigDocument parseString(String s) {
        return parseString(s, ConfigParseOptions.defaults());
    }
}
