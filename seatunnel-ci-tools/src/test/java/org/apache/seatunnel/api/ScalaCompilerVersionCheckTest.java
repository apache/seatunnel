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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;

/**
 * Verifies that {@code seatunnel.shade.scala-compiler.version} stays in sync with {@code
 * scala.version} in the root pom.xml. When the root scala version is bumped, the shade
 * scala-compiler version must be updated to match, since both must use the same Scala compiler.
 */
public class ScalaCompilerVersionCheckTest {

    @Test
    public void testScalaCompilerShadeVersionMatchesRootScalaVersion() throws Exception {
        File rootPom = new File("../pom.xml");
        Document pom = parsePom(rootPom);

        String scalaVersion = getPropertyValue(pom, "scala.version");
        String shadeScalaCompilerVersion =
                getPropertyValue(pom, "seatunnel.shade.scala-compiler.version");

        Assertions.assertNotNull(scalaVersion, "scala.version property not found in root pom.xml");
        Assertions.assertNotNull(
                shadeScalaCompilerVersion,
                "seatunnel.shade.scala-compiler.version property not found in root pom.xml");
        String scalaBinaryVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf('.'));
        Assertions.assertEquals(
                scalaBinaryVersion,
                shadeScalaCompilerVersion,
                "seatunnel.shade.scala-compiler.version must match the binary version (major.minor) of scala.version. "
                        + "scala.version is '"
                        + scalaVersion
                        + "', so seatunnel.shade.scala-compiler.version should be '"
                        + scalaBinaryVersion
                        + "'. "
                        + "Update seatunnel.shade.scala-compiler.version when bumping scala.version.");
    }

    private Document parsePom(File pomFile) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setNamespaceAware(false);
        return factory.newDocumentBuilder().parse(pomFile);
    }

    private String getPropertyValue(Document pom, String propertyName) {
        NodeList properties = pom.getDocumentElement().getChildNodes();
        for (int i = 0; i < properties.getLength(); i++) {
            Node node = properties.item(i);
            if ("properties".equals(node.getNodeName())) {
                NodeList children = node.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    if (propertyName.equals(children.item(j).getNodeName())) {
                        return children.item(j).getTextContent().trim();
                    }
                }
            }
        }
        return null;
    }
}
