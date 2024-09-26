package org.apache.seatunnel.plugin.discovery;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("unchecked")
public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {

    protected final BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer;
    protected final Config pluginMappingConfig;

    protected final ConcurrentHashMap<PluginIdentifier, Optional<URL>> pluginJarPath =
            new ConcurrentHashMap<>(Common.COLLECTION_SIZE);

    public AbstractPluginDiscovery(
            Config pluginMappingConfig, BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer) {
        this.addURLToClassLoaderConsumer = addURLToClassLoaderConsumer;
        this.pluginMappingConfig = pluginMappingConfig;
    }

    @Override
    public List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
                .map(this::getPluginJarPath)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
                .map(this::createPluginInstance)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public Optional<T> createOptionalPluginInstance(
            PluginIdentifier pluginIdentifier, Collection<URL> pluginJars) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        T pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
        if (pluginInstance != null) {
            log.info("Load plugin: {} from classpath", pluginIdentifier);
            return Optional.of(pluginInstance);
        }
        Optional<URL> pluginJarPath = getPluginJarPath(pluginIdentifier);
        // if the plugin jar not exist in classpath, will load from plugin dir.
        if (pluginJarPath.isPresent()) {
            try {
                // use current thread classloader to avoid different classloader load same class
                // error.
                this.addURLToClassLoaderConsumer.accept(classLoader, pluginJarPath.get());
                for (URL jar : pluginJars) {
                    addURLToClassLoaderConsumer.accept(classLoader, jar);
                }
            } catch (Exception e) {
                log.warn(
                        "can't load jar use current thread classloader, use URLClassLoader instead now."
                                + " message: "
                                + e.getMessage());
                URL[] urls = new URL[pluginJars.size() + 1];
                int i = 0;
                for (URL pluginJar : pluginJars) {
                    urls[i++] = pluginJar;
                }
                urls[i] = pluginJarPath.get();
                classLoader =
                        new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
            }
            pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
            if (pluginInstance != null) {
                log.info(
                        "Load plugin: {} from path: {} use classloader: {}",
                        pluginIdentifier,
                        pluginJarPath.get(),
                        classLoader.getClass().getName());
                return Optional.of(pluginInstance);
            }
        }
        return Optional.empty();
    }

    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier) {
        return (T) createPluginInstance(pluginIdentifier, Collections.EMPTY_LIST);
    }

    @Override
    public Optional<T> createOptionalPluginInstance(PluginIdentifier pluginIdentifier) {
        return createOptionalPluginInstance(pluginIdentifier, Collections.EMPTY_LIST);
    }

    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier, Collection<URL> pluginJars) {
        Optional<T> instance = createOptionalPluginInstance(pluginIdentifier, pluginJars);
        if (instance.isPresent()) {
            return instance.get();
        }
        throw new RuntimeException("Plugin " + pluginIdentifier + " not found.");
    }

    @Override
    public ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> getOptionRules(
            String pluginIdentifier) {
        Optional<Map.Entry<PluginIdentifier, OptionRule>> pluginEntry =
                getPlugins().entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getKey()
                                                .getPluginName()
                                                .equalsIgnoreCase(pluginIdentifier))
                        .findFirst();
        if (pluginEntry.isPresent()) {
            Map.Entry<PluginIdentifier, OptionRule> entry = pluginEntry.get();
            List<Option<?>> requiredOptions =
                    entry.getValue().getRequiredOptions().stream()
                            .flatMap(requiredOption -> requiredOption.getOptions().stream())
                            .collect(Collectors.toList());
            List<Option<?>> optionalOptions = entry.getValue().getOptionalOptions();
            return ImmutableTriple.of(entry.getKey(), requiredOptions, optionalOptions);
        }
        return ImmutableTriple.of(null, new ArrayList<>(), new ArrayList<>());
    }

    protected T loadPluginInstance(PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (T t : serviceLoader) {
            if (t instanceof PluginIdentifierInterface) {
                // new api
                PluginIdentifierInterface pluginIdentifierInstance = (PluginIdentifierInterface) t;
                if (StringUtils.equalsIgnoreCase(
                        pluginIdentifierInstance.getPluginName(),
                        pluginIdentifier.getPluginName())) {
                    return (T) pluginIdentifierInstance;
                }
            } else {
                throw new UnsupportedOperationException(
                        "Plugin instance: " + t + " is not supported.");
            }
        }
        return null;
    }

    /**
     * Get the plugin instance.
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin instance.
     */
    protected Optional<URL> getPluginJarPath(PluginIdentifier pluginIdentifier) {
        return pluginJarPath.computeIfAbsent(pluginIdentifier, this::findPluginJarPath);
    }

    protected static <T> T findMostSimlarPluginJarFile(
            T[] pluginFiles, Function<T, String> func, String pluginJarPrefix) {
        String splitRegex = "\\-|\\_|\\.";
        double maxSimlarity = -Integer.MAX_VALUE;
        int mostSimlarPluginJarFileIndex = -1;
        for (int i = 0; i < pluginFiles.length; i++) {
            String filename = func.apply(pluginFiles[i]);
            double similarity =
                    CosineSimilarityUtil.cosineSimilarity(pluginJarPrefix, filename, splitRegex);
            if (similarity > maxSimlarity) {
                maxSimlarity = similarity;
                mostSimlarPluginJarFileIndex = i;
            }
        }
        return pluginFiles[mostSimlarPluginJarFileIndex];
    }

    /**
     * Get all support plugin already in SEATUNNEL_HOME, support connector-v2 and transform-v2
     *
     * @param pluginType
     * @param factoryIdentifier
     * @param optionRule
     * @return
     */
    protected void getPluginsByFactoryIdentifier(
            LinkedHashMap<PluginIdentifier, OptionRule> plugins,
            PluginType pluginType,
            String factoryIdentifier,
            OptionRule optionRule) {
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of("seatunnel", pluginType.getType(), factoryIdentifier);
        plugins.computeIfAbsent(pluginIdentifier, k -> optionRule);
    }

    /**
     * Find the plugin jar path;
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin jar path.
     */
    protected abstract Optional<URL> findPluginJarPath(PluginIdentifier pluginIdentifier);

    /**
     * Get spark plugin interface.
     *
     * @return plugin base class.
     */
    protected abstract Class<T> getPluginBaseClass();

    protected abstract List<Factory> getPluginFactories();

    static class CosineSimilarityUtil {
        public static double cosineSimilarity(String textA, String textB, String splitRegrex) {
            Set<String> words1 =
                    new HashSet<>(Arrays.asList(textA.toLowerCase().split(splitRegrex)));
            Set<String> words2 =
                    new HashSet<>(Arrays.asList(textB.toLowerCase().split(splitRegrex)));
            int[] termFrequency1 = calculateTermFrequencyVector(textA, words1, splitRegrex);
            int[] termFrequency2 = calculateTermFrequencyVector(textB, words2, splitRegrex);
            return calculateCosineSimilarity(termFrequency1, termFrequency2);
        }

        private static int[] calculateTermFrequencyVector(
                String text, Set<String> words, String splitRegrex) {
            int[] termFrequencyVector = new int[words.size()];
            String[] textArray = text.toLowerCase().split(splitRegrex);
            List<String> orderedWords = new ArrayList<String>();
            words.clear();
            for (String word : textArray) {
                if (!words.contains(word)) {
                    orderedWords.add(word);
                    words.add(word);
                }
            }
            for (String word : textArray) {
                if (words.contains(word)) {
                    int index = 0;
                    for (String w : orderedWords) {
                        if (w.equals(word)) {
                            termFrequencyVector[index]++;
                            break;
                        }
                        index++;
                    }
                }
            }
            return termFrequencyVector;
        }

        private static double calculateCosineSimilarity(int[] vectorA, int[] vectorB) {
            double dotProduct = 0.0;
            double magnitudeA = 0.0;
            double magnitudeB = 0.0;
            int vectorALength = vectorA.length;
            int vectorBLength = vectorB.length;
            if (vectorALength < vectorBLength) {
                int[] vectorTemp = new int[vectorBLength];
                for (int i = 0; i < vectorB.length; i++) {
                    if (i <= vectorALength - 1) {
                        vectorTemp[i] = vectorA[i];
                    } else {
                        vectorTemp[i] = 0;
                    }
                }
                vectorA = vectorTemp;
            }
            if (vectorALength > vectorBLength) {
                int[] vectorTemp = new int[vectorALength];
                for (int i = 0; i < vectorA.length; i++) {
                    if (i <= vectorBLength - 1) {
                        vectorTemp[i] = vectorB[i];
                    } else {
                        vectorTemp[i] = 0;
                    }
                }
                vectorB = vectorTemp;
            }
            for (int i = 0; i < vectorA.length; i++) {
                dotProduct += vectorA[i] * vectorB[i];
                magnitudeA += Math.pow(vectorA[i], 2);
                magnitudeB += Math.pow(vectorB[i], 2);
            }

            magnitudeA = Math.sqrt(magnitudeA);
            magnitudeB = Math.sqrt(magnitudeB);

            if (magnitudeA == 0 || magnitudeB == 0) {
                return 0.0; // Avoid dividing by 0
            } else {
                return dotProduct / (magnitudeA * magnitudeB);
            }
        }
    }
}
