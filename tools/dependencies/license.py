#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import platform
import sys

if sys.version_info.major < 3:
    print('At least Python3 is required!')
    print('Please refer to the https://www.python.org/downloads/ documentation if you do not have a right Python env.')
    exit(-1)

if len(sys.argv) - 1 != 3:
    print("The length of arguments should be 3!")
    print("The first argument should be the path to the THIRD-PARTY.txt file.")
    print("The second argument should be the path to the LICENSE file.")
    print("The third argument should be a flag that controls whether to print the diff or change the LICENSE file.")
    exit(-1)

third_party = sys.argv[1]
license = sys.argv[2]
print_diff = sys.argv[3]

with open(third_party, "r") as f:
    licenses = f.readlines()

licenses_keyword_map = {
    "Apache 2.0 License": ["Apache", "APL2"],
    "MIT License": ["MIT"],
    "BSD License": ["BSD"],
    "CC0-1.0 License": ["CC0"],
    "CDDL License": ["CDDL"],
    "Eclipse Public License": ["Eclipse", "EDL"],
    "Public Domain License": ["Public Domain"],
    "Mozilla Public License Version 2.0": ["Mozilla Public License"],
    "Go License": ["The Go license"],
    "Unicode/ICU License": ["Unicode License", "ICU", "Unicode/ICU License"]
}
dependency_licenses_map = {
    "commons-beanutils:commons-beanutils:1.7.0": "(Apache License, Version 2.0) Apache Commons BeanUtils (commons-beanutils:commons-beanutils:1.7.0 - https://commons.apache.org/proper/commons-beanutils/)",
    "commons-pool:commons-pool:1.5.4": "(The Apache Software License, Version 2.0) Commons Pool (commons-pool:commons-pool:1.5.4 - http://commons.apache.org/pool/)",
    "org.antlr:antlr-runtime:3.4": "(BSD licence) ANTLR 3 Runtime (org.antlr:antlr-runtime:3.4 - http://www.antlr.org)",
    "javax.transaction:jta:1.1": "(CDDL + GPLv2 with classpath exception) Java Transaction API (javax.transaction:jta:1.1 - http://java.sun.com/products/jta)",
    "javax.servlet.jsp:jsp-api:2.1": "(CDDL + GPLv2 with classpath exception) Java Servlet API (javax.servlet.jsp:jsp-api:2.1 - https://javaee.github.io/javaee-jsp-api)",
    "javax.servlet:servlet-api:2.5": "(CDDL + GPLv2 with classpath exception) Java Servlet API (javax.servlet:servlet-api:2.5 - http://servlet-spec.java.net)",
    "oro:oro:2.0.8": "(Apache License, Version 1.1) ORO (oro:oro:2.0.8 - https://mvnrepository.com/artifact/oro/oro)",
    "org.hyperic:sigar:1.6.5.132": "(Apache License, Version 2.0) Sigar (org.hyperic:sigar:1.6.5.132 - https://github.com/hyperic/sigar)",
    "asm:asm:3.1": "(BSD License) ASM (asm:asm:3.1 - https://asm.ow2.io/license.html)",
    "com.ibm.icu:icu4j:55.1": "(Unicode/ICU License) ICU4J (com.ibm.icu:icu4j:55.1 - http://icu-project.org/)",
    "jakarta.activation:jakarta.activation-api:1.2.1": "(EDL 1.0) Jakarta Activation API (jakarta.activation:jakarta.activation-api:1.2.1 - https://github.com/eclipse-ee4j/jaf)",
    "org.apache.zookeeper:zookeeper:3.3.1": "(Apache License, Version 2.0) Apache ZooKeeper - Server (org.apache.zookeeper:zookeeper:3.3.1 - http://zookeeper.apache.org/zookeeper)",
    "org.apache.zookeeper:zookeeper:3.4.6": "(Apache License, Version 2.0) Apache ZooKeeper - Server (org.apache.zookeeper:zookeeper:3.4.6 - http://zookeeper.apache.org/zookeeper)",
    "org.codehaus.jettison:jettison:1.1": "(Apache License, Version 2.0) Jettison (org.codehaus.jettison:jettison:1.1 - https://github.com/jettison-json/jettison)"
}
licenses_describe_map = {
    "Apache 2.0 License":
        """The following components are provided under the Apache License. See project link for details.
The text of each license is the standard Apache 2.0 license.
""",
    "MIT License": """The following components are provided under the MIT License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "BSD License": """The following components are provided under a BSD license. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "CC0-1.0 License": """The following components are provided under the CC0-1.0 License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "CDDL License": """The following components are provided under the CDDL License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "Eclipse Public License": """The following components are provided under the Eclipse Public License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "Public Domain License": """The following components are provided under the Public Domain License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "Mozilla Public License Version 2.0": """The following components are provided under the Mozilla Public License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "Unicode/ICU License": """The following components are provided under the Unicode/ICU License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
""",
    "Go License": """The following components are provided under the Go License. See project link for details.
The text of each license is also included at licenses/LICENSE-[project].txt.
"""
}
licenses_map = {
    "Apache 2.0 License": [],
    "MIT License": [],
    "BSD License": [],
    "CC0-1.0 License": [],
    "CDDL License": [],
    "Eclipse Public License": [],
    "Public Domain License": [],
    "Mozilla Public License Version 2.0": [],
    "Unicode/ICU License": [],
    "Go License": [],
    "Other License": []
}

for _ in licenses:
    # Because the license of this project itself dose not need to be declared here
    if "org.apache.seatunnel" in _:
        continue
    if "Unknown license" in _:
        for k, v in dependency_licenses_map.items():
            if k in _:
                _ = v
                break
    _ = _.strip(" ")
    if _ == '\n':
        continue
    if '(' not in _ or ')' not in _:
        continue
    # (Apache 2.0 License) Spark Project Tags (org.apache.spark:spark-tags_2.11:2.4.0 - http://spark.apache.org/)
    items = _.split(") ")
    if len(items) != 2:
        continue
    type = items[0]
    l = None
    for k in licenses_keyword_map:
        for keyword in licenses_keyword_map[k]:
            if keyword in type:
                l = k
                break
    if l is None:
        l = "Other License"
    licenses_map[l].append(_.strip('\n'))

if len(licenses_map["Other License"]) != 0:
    for other_license in licenses_map["Other License"]:
        print(other_license)
    print("Please confirm the license by finding LICENSE file in the corresponding Jar file and maintain it in the dependency_licenses_map instance.")
    exit(-1)

res = ""
res += """                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "{}"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright {yyyy} {name of copyright owner}

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

=======================================================================
Apache SeaTunnel Subcomponents:

The Apache SeaTunnel project contains subcomponents with separate copyright
notices and license terms. Your use of the source code for the these
subcomponents is subject to the terms and conditions of the following
licenses.



"""

for k, v in licenses_map.items():
    if len(v) == 0:
        continue
    res += "========================================================================\n"
    res += k
    res += '\n'
    res += "========================================================================\n\n"
    res += licenses_describe_map[k]
    res += '\n'
    for _ in sorted(v):
        res += "     "
        res += _
        res += '\n'
    res += '\n\n'

if print_diff == 'true':
    tmp_file = third_party + ".tmp"
    with open(tmp_file, "w") as f:
        f.write(res)
    print("Please modify the LICENSE file according to the diff information.")
    if platform.system() == "Windows":
        diff_res = os.system("FC " + license + " " + tmp_file)
    else:
        diff_res = os.system("diff " + license + " " + tmp_file)
    if int(diff_res) != 0:
        print("Failed.")
        exit(-1)
    else:
        print("Successful.")
else:
    with open(license, "w") as f:
        f.write(res)
