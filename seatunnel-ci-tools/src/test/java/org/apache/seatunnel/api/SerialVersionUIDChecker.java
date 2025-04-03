package org.apache.seatunnel.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.Type;
import com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration;
import com.github.javaparser.resolution.types.ResolvedReferenceType;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SerialVersionUIDChecker.TestResultLogger.class)
public class SerialVersionUIDChecker {
    private static final Logger LOG = LoggerFactory.getLogger(SerialVersionUIDChecker.class);
    private static final String JAVA_FILE_EXTENSION = ".java";
    private static final String CONNECTOR_DIR = "seatunnel-connectors-v2";
    private static final String JAVA_PATH_FRAGMENT =
            "src" + File.separator + "main" + File.separator + "java";
    private static final JavaParser JAVA_PARSER;
    private static final Set<String> checkedClasses = new HashSet<>();

    static {
        CombinedTypeSolver typeSolver = new CombinedTypeSolver();
        typeSolver.add(new ReflectionTypeSolver());

        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            paths.filter(path -> path.toString().contains("src/main/java"))
                    .forEach(
                            path -> {
                                try {
                                    typeSolver.add(new JavaParserTypeSolver(path.toFile()));
                                } catch (Exception e) {
                                }
                            });
        } catch (IOException e) {
            LOG.error("Failed to setup type solver", e);
        }

        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(typeSolver);
        JAVA_PARSER = new JavaParser();
        JAVA_PARSER.getParserConfiguration().setSymbolResolver(symbolSolver);
    }

    @Test
    public void checkSerialVersionUID() {
        List<String> missingSerialVersionUID = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            List<Path> connectorClassPaths =
                    paths.filter(
                                    path -> {
                                        String pathString = path.toString();
                                        return pathString.endsWith(JAVA_FILE_EXTENSION)
                                                && pathString.contains(CONNECTOR_DIR)
                                                && pathString.contains(JAVA_PATH_FRAGMENT);
                                    })
                            .collect(Collectors.toList());

            LOG.info("Found {} connector class files to check", connectorClassPaths.size());

            for (Path path : connectorClassPaths) {
                try {
                    ParseResult<CompilationUnit> parseResult =
                            JAVA_PARSER.parse(Files.newInputStream(path));
                    parseResult
                            .getResult()
                            .ifPresent(
                                    compilationUnit -> {
                                        List<ClassOrInterfaceDeclaration> classes =
                                                compilationUnit.findAll(
                                                        ClassOrInterfaceDeclaration.class);
                                        for (ClassOrInterfaceDeclaration classDeclaration :
                                                classes) {
                                            boolean implementsSeaTunnelSourceOrSink =
                                                    classDeclaration.getImplementedTypes().stream()
                                                            .anyMatch(
                                                                    type -> {
                                                                        String typeName =
                                                                                type
                                                                                        .getNameAsString();
                                                                        return typeName.equals(
                                                                                        "SeaTunnelSource")
                                                                                || typeName.equals(
                                                                                        "SeaTunnelSink");
                                                                    });

                                            if (implementsSeaTunnelSourceOrSink) {
                                                classDeclaration
                                                        .getImplementedTypes()
                                                        .forEach(
                                                                implementedType -> {
                                                                    implementedType
                                                                            .getTypeArguments()
                                                                            .ifPresent(
                                                                                    typeArgs -> {
                                                                                        for (Type
                                                                                                typeArg :
                                                                                                        typeArgs) {
                                                                                            if (typeArg
                                                                                                    .isClassOrInterfaceType()) {
                                                                                                ClassOrInterfaceType
                                                                                                        classType =
                                                                                                                typeArg
                                                                                                                        .asClassOrInterfaceType();
                                                                                                try {
                                                                                                    ResolvedReferenceType
                                                                                                            resolvedType =
                                                                                                                    classType
                                                                                                                            .resolve()
                                                                                                                            .asReferenceType();
                                                                                                    if (resolvedType
                                                                                                            == null) {
                                                                                                        continue;
                                                                                                    }

                                                                                                    List<
                                                                                                                    ResolvedReferenceType>
                                                                                                            allAncestors =
                                                                                                                    resolvedType
                                                                                                                            .getAllAncestors();

                                                                                                    boolean
                                                                                                            isSerializable =
                                                                                                                    resolvedType
                                                                                                                                    .getQualifiedName()
                                                                                                                                    .equals(
                                                                                                                                            "java.io.Serializable")
                                                                                                                            || allAncestors
                                                                                                                                    .stream()
                                                                                                                                    .anyMatch(
                                                                                                                                            ancestor ->
                                                                                                                                                    ancestor.getQualifiedName()
                                                                                                                                                            .equals(
                                                                                                                                                                    "java.io.Serializable"));

                                                                                                    if (isSerializable) {
                                                                                                        ResolvedReferenceTypeDeclaration
                                                                                                                typeDeclaration =
                                                                                                                        resolvedType
                                                                                                                                .getTypeDeclaration()
                                                                                                                                .orElse(
                                                                                                                                        null);
                                                                                                        if (typeDeclaration
                                                                                                                == null) {
                                                                                                            continue;
                                                                                                        }
                                                                                                        String
                                                                                                                paramTypeName =
                                                                                                                        typeDeclaration
                                                                                                                                .getQualifiedName();

                                                                                                        if (!checkedClasses
                                                                                                                .contains(
                                                                                                                        paramTypeName)) {
                                                                                                            boolean
                                                                                                                    hasSerialVersionUID =
                                                                                                                            false;

                                                                                                            if (typeDeclaration
                                                                                                                    .isInterface()) {
                                                                                                                hasSerialVersionUID =
                                                                                                                        true;
                                                                                                            } else {
                                                                                                                hasSerialVersionUID =
                                                                                                                        typeDeclaration
                                                                                                                                .getAllFields()
                                                                                                                                .stream()
                                                                                                                                .anyMatch(
                                                                                                                                        field ->
                                                                                                                                                field.getName()
                                                                                                                                                        .equals(
                                                                                                                                                                "serialVersionUID"));
                                                                                                            }

                                                                                                            if (!hasSerialVersionUID) {
                                                                                                                missingSerialVersionUID
                                                                                                                        .add(
                                                                                                                                paramTypeName);
                                                                                                                LOG
                                                                                                                        .warn(
                                                                                                                                "Class {} is missing serialVersionUID field",
                                                                                                                                paramTypeName);
                                                                                                            }

                                                                                                            checkedClasses
                                                                                                                    .add(
                                                                                                                            paramTypeName);
                                                                                                        }
                                                                                                    }
                                                                                                } catch (
                                                                                                        Exception
                                                                                                                e) {
                                                                                                    LOG
                                                                                                            .warn(
                                                                                                                    "Could not resolve type: {} in file: {}",
                                                                                                                    classType
                                                                                                                            .getNameAsString(),
                                                                                                                    path,
                                                                                                                    e);
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    });
                                                                });
                                            }
                                        }
                                    });
                } catch (IOException e) {
                    LOG.warn("Could not parse file: {}", path, e);
                }
            }

            LOG.info("Check completed. Checked {} connector classes.", connectorClassPaths.size());

            if (!missingSerialVersionUID.isEmpty()) {
                StringBuilder errorMessage = new StringBuilder();
                errorMessage.append(
                        "=================================================================\n");
                errorMessage.append(
                        "Test failed: The following classes are missing serialVersionUID fields or have a serialVersionUID value of -1L\n");
                errorMessage.append(
                        "=================================================================\n");
                errorMessage
                        .append("A total of ")
                        .append(missingSerialVersionUID.size())
                        .append(" Question:\n\n");

                for (int i = 0; i < missingSerialVersionUID.size(); i++) {
                    errorMessage
                            .append(i + 1)
                            .append(". ")
                            .append(missingSerialVersionUID.get(i))
                            .append("\n");
                }

                errorMessage.append(
                        "\n=================================================================\n");
                errorMessage.append(
                        "Please add a serialVersionUID field to the above class and make sure its value is not -1L, for example:\n");
                errorMessage.append(
                        "private static final long serialVersionUID = 5967888460683065669L;\n");
                errorMessage.append(
                        "=================================================================\n");

                LOG.error("Test failed: {}", errorMessage.toString());
                fail(errorMessage.toString());
            }

            LOG.info("All checked classes have correct serialVersionUID.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to walk through connector directories", e);
        }
    }

    public static class TestResultLogger implements TestWatcher {
        @Override
        public void testSuccessful(ExtensionContext context) {
            LOG.info("Test successful: {}", context.getDisplayName());
        }

        @Override
        public void testFailed(ExtensionContext context, Throwable cause) {
            LOG.error("Test failed: {}", context.getDisplayName(), cause);
        }
    }
}
