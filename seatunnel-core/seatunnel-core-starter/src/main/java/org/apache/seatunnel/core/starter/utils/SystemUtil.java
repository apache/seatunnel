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