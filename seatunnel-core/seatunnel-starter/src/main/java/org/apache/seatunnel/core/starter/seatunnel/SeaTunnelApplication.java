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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.seatunnel.args.ApplicationCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

/** User-facing entrypoint for native Zeta application deployment and management. */
public final class SeaTunnelApplication {

    public static void main(String[] args) {
        ApplicationCommandArgs applicationCommandArgs =
                CommandLineUtils.parse(
                        args,
                        new ApplicationCommandArgs(),
                        EngineType.SEATUNNEL_APPLICATION.getStarterShellName(),
                        false);
        try {
            SeaTunnel.run(applicationCommandArgs.buildCommand());
        } catch (Exception e) {
            System.err.println("Application command failed: " + e.getMessage());
            System.exit(1);
        }
    }
}
