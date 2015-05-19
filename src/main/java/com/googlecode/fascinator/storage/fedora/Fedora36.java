/*
 * The Fascinator - Fedora Commons 3.x storage plugin
 * Copyright (C) 2009-2011 University of Southern Queensland
 * Copyright (C) 2011 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.storage.fedora;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.BasicHttpClient;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.FedoraCredentials;
import com.yourmediashelf.fedora.client.request.GetObjectXML;
import com.yourmediashelf.fedora.client.response.FedoraResponse;

/**
 * A private utility class to wrap Fedora connectivity and save repeated
 * instantiations across the package. Many methods/properties here are default
 * scoped to package-private so Payloads, DigitalObjects and the top-level
 * Storage class all get access.
 *
 * @author Greg Pendlebury
 */
public class Fedora36 {
    /** Default Fedora base URL **/
    private static final String DEFAULT_URL = "http://localhost:8080/fedora/";

    /** Default Fedora namespace **/
    private static final String DEFAULT_NAMESPACE = "uuid";

    /** Test PID to retrieve from the server **/
    private static final String FEDORA_TEST_PID = "fedora-system:FedoraObject-3.0";

    /** Logger */
    private static Logger log = LoggerFactory.getLogger(Fedora36.class);

    /** System Config */
    private static JsonSimpleConfig systemConfig;

    /** Fedora - Client */
    private static FedoraClient fedoraClient;

    private static LinkedBlockingDeque<FedoraClient> ncFedoraClientCollection = new LinkedBlockingDeque<FedoraClient>(
            1);
    /** Fedora - API-A */
    // private static FedoraAPIA accessApi;

    /** Fedora - API-M */
    // private static FedoraAPIM managementApi;

    /** Fedora - Base URL */
    private static String fedoraUrl;

    /** Fedora - Get URL */
    private static String fedoraGetUrl;

    /** Fedora - Username */
    private static String fedoraUsername;

    /** Fedora - Password */
    private static String fedoraPassword;

    /** Fedora - Namespace */
    private static String fedoraNamespace;

    /** Fedora - Connection timeout */
    private static int fedoraTimeout;

    /** Fedora - Server version */
    private static String fedoraVersion;

    /** Fascinator HTTP Client */
    private static BasicHttpClient http;

    /** Open HTTP Connections */
    private static Map<String, List<GetMethod>> connections;

    /**
     * Public init method for File based configuration.
     *
     * @param jsonFile The File containing JSON configuration
     * @throws StorageException if any errors occur
     */
    static void init(File jsonFile) throws StorageException {
        try {
            systemConfig = new JsonSimpleConfig(jsonFile);
            init();
        } catch (IOException ioe) {
            throw new StorageException("Failed to read file configuration!",
                    ioe);
        }
    }

    /**
     * Public init method for String based configuration.
     *
     * @param jsonString The String containing JSON configuration
     * @throws StorageException if any errors occur
     */
    static void init(String jsonString) throws StorageException {
        try {
            systemConfig = new JsonSimpleConfig(jsonString);
            init();
        } catch (IOException ioe) {
            throw new StorageException("Failed to read string configuration!",
                    ioe);
        }
    }

    /**
     * Constructor
     *
     * @throws StorageException if any errors occur
     */
    private static void init() throws StorageException {
        // Don't instantiate twice
        if (fedoraClient != null) {
            return;
        }

        // Grab all our information from config
        fedoraUrl = systemConfig.getString(DEFAULT_URL, "storage", "fedora36",
                "url");
        fedoraUsername = systemConfig.getString(null, "storage", "fedora36",
                "username");
        fedoraPassword = systemConfig.getString(null, "storage", "fedora36",
                "password");
        fedoraNamespace = systemConfig.getString(DEFAULT_NAMESPACE, "storage",
                "fedora36", "namespace");
        fedoraTimeout = systemConfig.getInteger(15, "storage", "fedora36",
                "timeout");
        if (fedoraUrl == null || fedoraNamespace == null
                || fedoraUsername == null || fedoraPassword == null) {
            throw new StorageException("Fedora Storage:"
                    + " Valid Fedora configuration is mising!");
        }

        // Sort out our base URL and HTTP client
        if (!fedoraUrl.endsWith("/")) {
            fedoraUrl += "/";
        }
        fedoraGetUrl = fedoraUrl + "get/";
        http = new BasicHttpClient(fedoraUrl);
        http.authenticate(fedoraUsername, fedoraPassword);
        connections = new HashMap<String, List<GetMethod>>();
        // Will throw the StorageException for us if there's something wrong
        fedoraConnect();
    }

    /**
     * Establish a connection to Fedora's management API (API-M) to confirm
     * credentials, then return the instantiated fedora client used to connect.
     *
     * @return FedoraClient : The client used to connect to the API
     * @throws StorageException if there was an error
     */
    private static FedoraClient fedoraConnect() throws StorageException {
        if (fedoraClient != null) {
            return fedoraClient;
        }

        try {
            // Connect to the server
            FedoraCredentials credentials = new FedoraCredentials(fedoraUrl,
                    fedoraUsername, fedoraPassword);
            fedoraClient = new FedoraClient(credentials);

            // Because this is a new connection we're going
            // to do some additional work (and logging)
            log.info("Connected to FEDORA : '{}'", fedoraUrl);
            // Make sure we can get the server version
            fedoraVersion = fedoraClient.getServerVersion();
            log.info("FEDORA version: '{}'", fedoraVersion);
            // And that we have appropriate access to the management API

            // Version cutout
            if (!fedoraVersion.startsWith("3.")) {
                throw new StorageException("Error; this plugin is designed"
                        + " to work with Fedora versions 3.x");
            }

            // Version 3.x credentials test..
            GetObjectXML getObjectXML = FedoraClient
                    .getObjectXML(FEDORA_TEST_PID);
            FedoraResponse response = getObjectXML.execute(fedoraClient);
            String data = response.getEntity(String.class);
            if (data != null && data.getBytes().length > 0) {
                log.info("Access checked: '{}' = {} bytes", FEDORA_TEST_PID,
                        data.getBytes().length);
            } else {
                throw new StorageException("Error; could not retrieve "
                        + FEDORA_TEST_PID);
            }
        } catch (MalformedURLException ex) {
            throw new StorageException("Fedora Storage:"
                    + " Server URL is Invalid (?) : ", ex);
        } catch (IOException ex) {
            throw new StorageException("Fedora Storage:"
                    + " Error connecting to Fedora! : ", ex);
        } catch (Exception ex) {
            throw new StorageException("Fedora Storage:"
                    + " Error accesing management API! : ", ex);
        }

        ncFedoraClientCollection.addFirst(fedoraClient);
        return fedoraClient;
    }

    /**
     * Package-private version check of the connected server.
     *
     * @return String The Fedora Server's version
     */
    static String getVersion() {
        return fedoraVersion;
    }

    /**
     * Package-private 'getter' method for the base Fedora Client.
     *
     * @return FedoraClient The Fedora Client Object
     * @throws StorageException if any errors occur
     */
    static FedoraClient getClient() throws StorageException {
        return fedoraConnect();
    }

    /**
     * Package-private 'getter' method for the base Fedora Client.
     *
     * @return FedoraClient The Fedora Client Object
     * @throws StorageException if any errors occur
     * @throws InterruptedException
     */
    static FedoraClient getNCClient() throws StorageException {
        fedoraConnect();

        try {
            return ncFedoraClientCollection.takeFirst();
        } catch (InterruptedException e) {
            throw new StorageException(e);
        }

    }

    public static void releaseNCClient() {
        if (ncFedoraClientCollection.isEmpty()) {
            ncFedoraClientCollection.addFirst(fedoraClient);
        }

    }

    /**
     * Trivial 'getter' for retrieving the configured namespace.
     *
     * @return String The configured namespace Fascinator is using in Fedora
     */
    static String namespace() {
        return fedoraNamespace;
    }

    /**
     * Get InputStream of a Datastream via an authenticated web requested.
     *
     * @param fedoraPid The fedora PID containing the datastream
     * @param dsId The datastream's ID
     * @return InputStream An input stream for the datastream
     * @throws IOException if an errors occur
     */
    static InputStream getStream(String fedoraPid, String dsId)
            throws IOException {

        try {
            FedoraResponse r = FedoraClient.getDatastreamDissemination(
                    fedoraPid, dsId).execute(fedoraClient);
            return r.getEntityInputStream();
        } catch (FedoraClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Release an open HTTP connection that may be held for this combination of
     * PID and DSID.
     *
     * @param fedoraPid The fedora PID containing the datastream
     * @param dsId The datastream's ID
     */
    static void release(String fedoraPid, String dsId) {
        // log.debug("release({}, {})", fedoraPid, dsId);

        /*
         * This idea is worthwhile, but at this stage seems impractical.
         * We need some way of defining a key that is targetable to an
         * individual connection, not just to a payload.
         *
         * The API needs to be changed to accomodate this sort of thing. It
         * would most likely involve return a result object that provides
         * access to both the stream and a token that can be used on return.
         *
         * =============
         * TODO: Another option
         * http://hc.apache.org/httpclient-3.x/apidocs/org/apache/commons/httpclient/HttpMethod.html#getResponseBodyAsStream%28%29
         *
         * "... null may be returned ... if this method was called previously
         * and the resulting stream was closed."
         * =====
         *
         * So, relying on the above logic You could keep lists of connections
         * against each key, and each close() call against the key would check
         * for null inputstreams per connection, closing those that it finds...
         *
         * This idea minimises opened connections hanging around, but doesn't
         * address closing the actual InputStreams... it only closes connections
         * where the user remembered to close the InputStream themselves.
         *
         */

        String key = fedoraPid + "/" + dsId;
        if (connections.containsKey(key)) {
            // Prepare
            List<Integer> toClose = new ArrayList<Integer>();
            List<GetMethod> toTest = connections.get(key);
            Integer limit = toTest.size();
            // Loop through all connections
            for (Integer i = 0; i < limit; i++) {
                boolean close = false;
                try {
                    InputStream in = toTest.get(i).getResponseBodyAsStream();
                    // Case one, the InputStream has been closed already
                    if (in == null) {
                        close = true;
                    }
                } catch (IOException ioe) {
                    // Case two... some unknown error
                    close = true;
                }
                // We need to close this OUTSIDE THE LOOP
                if (close) {
                    toClose.add(i);
                }
            }

            // Second loop... and backwards to preserve index orders
            // as we remove higher values
            limit = toClose.size();
            for (Integer i = limit - 1; i >= 0; i--) {
                // Close
                toTest.get(i).releaseConnection();
                // Then remove
                toTest.remove((int) i);
            }
        }
    }

    /**
     * Build's a GET URL for the requested PID and DSID combination.
     *
     * @param fedoraPid The fedora PID containing the datastream
     * @param dsId The datastream's ID
     * @return String The URL, possibly null if not possible to encode
     */
    private static String buildGetUrl(String fedoraPid, String dsId) {
        try {
            String returnValue = fedoraGetUrl;
            returnValue += URLEncoder.encode(fedoraPid, "UTF-8");
            if (dsId != null) {
                returnValue += "/";
                returnValue += URLEncoder.encode(dsId, "UTF-8");
            }
            return returnValue;
        } catch (UnsupportedEncodingException ex) {
            log.error("Error encoding URL for object '{}' and DS '{}'",
                    fedoraPid, dsId);
            return null;
        }
    }

    /**
     * A really simple wrapper on closable object to allow trivial close
     * attempts when we are unsure if they are even open.
     *
     * @param toClose A Closeable Object to try closing
     */
    static void close(Closeable toClose) {
        try {
            toClose.close();
        } catch (IOException ex) {
            // No worries, they may not even be open
        }
    }

}
