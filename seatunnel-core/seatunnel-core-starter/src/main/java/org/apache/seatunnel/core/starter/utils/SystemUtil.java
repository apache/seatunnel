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
 
package org.apache.seatunnel.core.starter.utils;

import org.apache.commons.lang3.SystemUtils;

public class SystemUtil {
      
    public static String GetOsType() {
        String os_type="";
        
        if (SystemUtils.IS_OS_WINDOWS) {
           os_type="Windows";
        } else if (SystemUtils.IS_OS_MAC) {
           os_type="Mac";
        } else if (SystemUtils.IS_OS_LINUX) {
           os_type="Linux";
        } else if (SystemUtils.IS_OS_SOLARIS) {
           os_type="Solaris";
        } else {
          os_type="Unknown";
        }
        
        return os_type;
    }    
}