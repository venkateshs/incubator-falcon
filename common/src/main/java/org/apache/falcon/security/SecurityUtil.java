/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.security;

import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Security Util - bunch of security related helper methods.
 * Also doles out proxied UserGroupInformation. Caches proxied users.
 */
public final class SecurityUtil {

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String CONFIG_PREFIX = "falcon.authentication.";

    /**
     * Constant for the configuration property that indicates the authentication type.
     */
    public static final String AUTHENTICATION_TYPE = CONFIG_PREFIX + "type";

    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    /**
     * Constant for the configuration property that indicates the Name node principal.
     * This is used to talk to Hive Meta Store during parsing and validations only.
     */
    public static final String HIVE_METASTORE_PRINCIPAL = "hive.metastore.kerberos.principal";


    private static ConcurrentMap<String, UserGroupInformation> userUgiMap =
            new ConcurrentHashMap<String, UserGroupInformation>();

    private SecurityUtil() {
    }

    public static String getAuthenticationType() {
        return StartupProperties.get().getProperty(
                AUTHENTICATION_TYPE, PseudoAuthenticationHandler.TYPE);
    }

    public static boolean isSecurityEnabled() {
        String authenticationType = StartupProperties.get().getProperty(
                AUTHENTICATION_TYPE, PseudoAuthenticationHandler.TYPE);

        final boolean useKerberos;
        if (authenticationType == null || PseudoAuthenticationHandler.TYPE.equals(authenticationType)) {
            useKerberos = false;
        } else if (KerberosAuthenticationHandler.TYPE.equals(authenticationType)) {
            useKerberos = true;
        } else {
            throw new IllegalArgumentException("Invalid attribute value for "
                    + AUTHENTICATION_TYPE + " of " + authenticationType);
        }

        return useKerberos;
    }

    public static UserGroupInformation getProxyUser(String proxyUser) throws IOException {
        UserGroupInformation proxyUgi = userUgiMap.get(proxyUser);
        if (proxyUgi == null) {
            // taking care of a race condition, the latest UGI will be discarded
            proxyUgi = UserGroupInformation.createProxyUser(proxyUser, UserGroupInformation.getLoginUser());
            userUgiMap.putIfAbsent(proxyUser, proxyUgi);
        }

        return proxyUgi;
    }

    public static String getLocalHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }
}
