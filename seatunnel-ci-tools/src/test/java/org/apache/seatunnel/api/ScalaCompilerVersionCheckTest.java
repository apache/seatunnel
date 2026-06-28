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

public class ScalaCompilerVersionCheckTest {

    @Test
    public void testScalaCompilerModuleUsesRootScalaVersion() throws Exception {
        File scalaCompilerPom = new File("../seatunnel-shade/seatunnel-scala-compiler/pom.xml");
        Document pom = parsePom(scalaCompilerPom);

        Assertions.assertFalse(
                hasDirectProperty(pom, "scala.version"),
                "seatunnel-scala-compiler must inherit scala.version from root pom.xml");
        Assertions.assertFalse(
                hasDirectProperty(pom, "scala.binary.version"),
                "seatunnel-scala-compiler must inherit scala.binary.version from root pom.xml");
        Assertions.assertEquals(
                "${scala.version}",
                findDependencyVersion(pom, "org.scala-lang", "scala-compiler"),
                "scala-compiler dependency must track the root scala.version property");
    }

    private Document parsePom(File pomFile) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setNamespaceAware(false);
        return factory.newDocumentBuilder().parse(pomFile);
    }

    private boolean hasDirectProperty(Document pom, String propertyName) {
        NodeList properties = pom.getDocumentElement().getChildNodes();
        for (int i = 0; i < properties.getLength(); i++) {
            Node node = properties.item(i);
            if ("properties".equals(node.getNodeName())) {
                NodeList children = node.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    if (propertyName.equals(children.item(j).getNodeName())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private String findDependencyVersion(Document pom, String groupId, String artifactId) {
        NodeList dependencies = pom.getElementsByTagName("dependency");
        for (int i = 0; i < dependencies.getLength(); i++) {
            Node dependency = dependencies.item(i);
            if (groupId.equals(childText(dependency, "groupId"))
                    && artifactId.equals(childText(dependency, "artifactId"))) {
                return childText(dependency, "version");
            }
        }
        Assertions.fail("Dependency not found: " + groupId + ":" + artifactId);
        return null;
    }

    private String childText(Node node, String childName) {
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (childName.equals(child.getNodeName())) {
                return child.getTextContent().trim();
            }
        }
        return null;
    }
}
