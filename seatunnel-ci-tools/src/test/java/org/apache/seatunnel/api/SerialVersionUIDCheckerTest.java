/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api;

import org.junit.jupiter.api.AfterAll;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SerialVersionUIDCheckerTest.TestResultLogger.class)
public class SerialVersionUIDCheckerTest {
    private static final Logger LOG = LoggerFactory.getLogger(SerialVersionUIDCheckerTest.class);
    private static final String JAVA_FILE_EXTENSION = ".java";
    private static final String CONNECTOR_DIR = "seatunnel-connectors-v2";
    private static final String JAVA_PATH_FRAGMENT =
            "src" + File.separator + "main" + File.separator + "java";
    private static final JavaParser JAVA_PARSER;
    private static final Set<String> checkedClasses = new HashSet<>();
    private static final Map<String, ClassOrInterfaceDeclaration> classDeclarationMap =
            new HashMap<>();

    static {
        CombinedTypeSolver typeSolver = new CombinedTypeSolver();
        typeSolver.add(new ReflectionTypeSolver());
        setupTypeSolver(typeSolver);
        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(typeSolver);
        JAVA_PARSER = new JavaParser();
        JAVA_PARSER.getParserConfiguration().setSymbolResolver(symbolSolver);
    }

    private static void setupTypeSolver(CombinedTypeSolver typeSolver) {
        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            paths.filter(path -> path.toString().contains("src/main/java"))
                    .forEach(
                            path -> {
                                try {
                                    typeSolver.add(new JavaParserTypeSolver(path.toFile()));
                                } catch (Exception e) {
                                    // ignore
                                }
                            });
        } catch (IOException e) {
            LOG.error("Failed to setup type solver", e);
        }
    }

    @Test
    public void checkSerialVersionUID() {
        List<String> missingSerialVersionUID = new ArrayList<>();
        List<Path> connectorClassPaths = findConnectorClassPaths();
        LOG.info("Found {} connector class files to check", connectorClassPaths.size());

        // First, populate the classDeclarationMap with all classes
        for (Path path : connectorClassPaths) {
            populateClassDeclarationMap(path);
        }
        LOG.info("Populated class declaration map with {} classes", classDeclarationMap.size());

        // Then check each class path for serialVersionUID
        for (Path path : connectorClassPaths) {
            checkClassPath(path, missingSerialVersionUID);
        }

        LOG.info("Check completed. Checked {} connector classes.", connectorClassPaths.size());
        if (!missingSerialVersionUID.isEmpty()) {
            String errorMessage = generateErrorMessage(missingSerialVersionUID);
            LOG.error("Test failed: {}", errorMessage);
            fail(errorMessage);
        }
        LOG.info("All checked classes have correct serialVersionUID.");
    }

    private List<Path> findConnectorClassPaths() {
        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            return paths.filter(
                            path -> {
                                String pathString = path.toString();
                                return pathString.endsWith(JAVA_FILE_EXTENSION)
                                        && pathString.contains(CONNECTOR_DIR)
                                        && pathString.contains(JAVA_PATH_FRAGMENT);
                            })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to walk through connector directories", e);
        }
    }

    /** Populate the classDeclarationMap with all class declarations from the given path. */
    private void populateClassDeclarationMap(Path path) {
        try {
            ParseResult<CompilationUnit> parseResult =
                    JAVA_PARSER.parse(Files.newInputStream(path));
            parseResult
                    .getResult()
                    .ifPresent(
                            compilationUnit -> {
                                List<ClassOrInterfaceDeclaration> classes =
                                        compilationUnit.findAll(ClassOrInterfaceDeclaration.class);
                                for (ClassOrInterfaceDeclaration classDeclaration : classes) {
                                    String className =
                                            classDeclaration.getFullyQualifiedName().orElse("");
                                    if (!className.isEmpty()) {
                                        classDeclarationMap.put(className, classDeclaration);
                                    }
                                }
                            });
        } catch (IOException e) {
            LOG.warn("Could not parse file: {}", path, e);
        }
    }

    /**
     * Check the class path for classes that implement SeaTunnelSource or SeaTunnelSink and verify
     * they have serialVersionUID.
     */
    private void checkClassPath(Path path, List<String> missingSerialVersionUID) {
        try {
            ParseResult<CompilationUnit> parseResult =
                    JAVA_PARSER.parse(Files.newInputStream(path));
            parseResult
                    .getResult()
                    .ifPresent(
                            compilationUnit -> {
                                List<ClassOrInterfaceDeclaration> classes =
                                        compilationUnit.findAll(ClassOrInterfaceDeclaration.class);
                                for (ClassOrInterfaceDeclaration classDeclaration : classes) {
                                    if (implementsSeaTunnelSourceOrSink(classDeclaration)) {
                                        checkImplementedTypes(
                                                classDeclaration, missingSerialVersionUID);
                                    }
                                }
                            });
        } catch (IOException e) {
            LOG.warn("Could not parse file: {}", path, e);
        }
    }

    private boolean implementsSeaTunnelSourceOrSink(ClassOrInterfaceDeclaration classDeclaration) {
        return classDeclaration.getImplementedTypes().stream()
                .anyMatch(
                        type -> {
                            String typeName = type.getNameAsString();
                            return typeName.equals("SeaTunnelSource")
                                    || typeName.equals("SeaTunnelSink");
                        });
    }

    private void checkImplementedTypes(
            ClassOrInterfaceDeclaration classDeclaration, List<String> missingSerialVersionUID) {
        classDeclaration
                .getImplementedTypes()
                .forEach(
                        implementedType -> {
                            implementedType
                                    .getTypeArguments()
                                    .ifPresent(
                                            typeArgs -> {
                                                for (Type typeArg : typeArgs) {
                                                    if (typeArg.isClassOrInterfaceType()) {
                                                        checkClassType(
                                                                typeArg.asClassOrInterfaceType(),
                                                                missingSerialVersionUID);
                                                    }
                                                }
                                            });
                        });
    }

    private void checkClassType(
            ClassOrInterfaceType classType, List<String> missingSerialVersionUID) {

        try {
            ResolvedReferenceType resolvedType = classType.resolve().asReferenceType();
            if (resolvedType == null) {
                return;
            }
            if (isSerializable(resolvedType)) {
                ResolvedReferenceTypeDeclaration typeDeclaration =
                        resolvedType.getTypeDeclaration().orElse(null);
                if (typeDeclaration == null) {
                    return;
                }
                String paramTypeName = typeDeclaration.getQualifiedName();
                if (!checkedClasses.contains(paramTypeName)) {
                    // Check if the class is abstract and return early if it is
                    if (isAbstractClass(typeDeclaration)) {
                        checkedClasses.add(paramTypeName);
                        return;
                    }

                    if (!hasSerialVersionUID(typeDeclaration)) {
                        missingSerialVersionUID.add(paramTypeName);
                        LOG.warn("Class {} is missing serialVersionUID field", paramTypeName);
                    }
                    checkedClasses.add(paramTypeName);
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve type: {} in file: {}", classType.getNameAsString(), e);
        }
    }

    private boolean isSerializable(ResolvedReferenceType resolvedType) {
        return resolvedType.getQualifiedName().equals("java.io.Serializable")
                || resolvedType.getAllAncestors().stream()
                        .anyMatch(
                                ancestor ->
                                        ancestor.getQualifiedName().equals("java.io.Serializable"));
    }

    private boolean hasSerialVersionUID(ResolvedReferenceTypeDeclaration typeDeclaration) {
        return typeDeclaration.isInterface()
                || typeDeclaration.getAllFields().stream()
                        .anyMatch(field -> field.getName().equals("serialVersionUID"));
    }

    private boolean isAbstractClass(ResolvedReferenceTypeDeclaration typeDeclaration) {
        // Only check classes, not interfaces
        if (!typeDeclaration.isClass()) {
            return false;
        }

        String className = typeDeclaration.getQualifiedName();

        // First check if we have the class declaration in our map
        ClassOrInterfaceDeclaration classDeclaration = classDeclarationMap.get(className);
        if (classDeclaration != null) {
            // Directly check if the class is abstract using the declaration
            return classDeclaration.isAbstract();
        }

        return false;
    }

    private String generateErrorMessage(List<String> missingSerialVersionUID) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append("=================================================================\n");
        errorMessage.append(
                "Test failed: The following classes are missing serialVersionUID fields\n");
        errorMessage.append("=================================================================\n");
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
        errorMessage.append("private static final long serialVersionUID = 5967888460683065669L;\n");
        errorMessage.append("=================================================================\n");
        return errorMessage.toString();
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

    @AfterAll
    public static void cleanup() {
        checkedClasses.clear();
        classDeclarationMap.clear();
    }
}
