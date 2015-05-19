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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.response.FedoraResponse;
import com.yourmediashelf.fedora.client.response.FindObjectsResponse;
import com.yourmediashelf.fedora.client.response.IngestResponse;

/**
 * <p>
 * This plugin provides storage using version 3.6 and above of the <a
 * href="http://www.fedora-commons.org/">Fedora Commons</a> Client. It has been
 * tested against v3.6 and 3.8 Fedora servers, but will probably support others
 * as well.
 * </p>
 *
 * <h3>Configuration</h3>
 * <table border="1">
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Required</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td>url</td>
 * <td>Base URL of a Fedora Commons server</td>
 * <td><b>Yes</b></td>
 * <td>http://localhost:8080/fedora</td>
 * </tr>
 * <tr>
 * <td>username</td>
 * <td>Fedora user account with read/write access</td>
 * <td><b>Yes</b> (depending on server setup)</td>
 * <td>fedoraAdmin</td>
 * </tr>
 * <tr>
 * <td>password</td>
 * <td>Password for the above user account</td>
 * <td><b>Yes</b> (depending on server setup)</td>
 * <td>fedoraAdmin</td>
 * </tr>
 * <tr>
 * <td>namespace</td>
 * <td>Namespace to use for Fedora Object PIDs</td>
 * <td>No</td>
 * <td>uuid</td>
 * </tr>
 * </table>
 *
 * <h3>Sample configuration</h3>
 *
 * <pre>
 * {
 *     "storage": {
 *         "type": "fedora36",
 *         "fedora36": {
 *             "url": "http://localhost:8080/fedora",
 *             "username": "fedoraAdmin",
 *             "password": "fedoraAdmin",
 *             "namespace": "uuid"
 *         }
 *     }
 * }
 * </pre>
 *
 * @author Linda Octalina
 * @author Oliver Lucido
 * @author Greg Pendlebury
 */
public class Fedora36Storage implements Storage {
    /** How many records are we ever willing to receive per result set */
    private static int SEARCH_ROW_LIMIT_PER_PAGE = 1000;

    /** FOXML Version String to send to Fedora */
    private static String FOXML_VERSION = "info:fedora/fedora-system:FOXML-1.1";

    /** Fedora log message for adding an object */
    private static String ADD_LOG_MESSAGE = "Fedora3DigitalObject added";

    /** Fedora log message for deleting an object */
    private static String DELETE_LOG_MESSAGE = "Fedora3DigitalObject deleted";

    /** Logger */
    private Logger log = LoggerFactory.getLogger(Fedora36Storage.class);

    /** System Config */
    private JsonSimpleConfig systemConfig;

    /** FOXML Template to use at object creation */
    private String foxmlTemplate;

    /**
     * Return the ID of this plugin.
     *
     * @return String the plugin's ID.
     */
    @Override
    public String getId() {
        return "fedora36";
    }

    /**
     * Return the name of this plugin.
     *
     * @return String the plugin's name.
     */
    @Override
    public String getName() {
        return "Fedora Commons 3.6+ Storage Plugin";
    }

    /**
     * Public init method for File based configuration.
     *
     * @param jsonFile The File containing JSON configuration
     * @throws StorageException if any errors occur
     */
    @Override
    public void init(File jsonFile) throws StorageException {
        try {
            systemConfig = new JsonSimpleConfig(jsonFile);
            Fedora36.init(jsonFile);
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
    @Override
    public void init(String jsonString) throws StorageException {
        try {
            systemConfig = new JsonSimpleConfig(jsonString);
            Fedora36.init(jsonString);
            init();
        } catch (IOException ioe) {
            throw new StorageException("Failed to read string configuration!",
                    ioe);
        }
    }

    /**
     * Initialisation occurs here
     *
     * @throws StorageException if any errors occur
     */
    private void init() throws StorageException {
        // A quick connection test
        Fedora36.getClient();

        // Do we have a template?
        String templatePath = systemConfig.getString(null, "storage",
                "fedora36", "foxmlTemplate");
        File templateFile = null;
        boolean deleteTempFile = false;
        if (templatePath != null) {
            templateFile = new File(templatePath);
        } else {
            try {
                templateFile = resourceToFile("foxml_template.xml");
                deleteTempFile = true;
            } catch (IOException ex) {
                throw new StorageException("Error; Unable to read new object "
                        + "template from JAR!", ex);
            }
        }

        // Read the template into a String
        if (!templateFile.exists()) {
            throw new StorageException("Error; The new object template"
                    + " provided does not exist: '" + templatePath + "'");
        }
        foxmlTemplate = fileToString(templateFile);
        if (deleteTempFile) {
            templateFile.delete();
        }
        if (foxmlTemplate == null) {
            throw new StorageException("Error; Unable to read new object "
                    + "template from disk: '" + templatePath + "'");
        }
    }

    /**
     * Not part of the API, but used in unit testing. Check the version of the
     * connected Fedora Server
     *
     * @throws String The Fedora Server's version
     */
    public String fedoraVersion() {
        return Fedora36.getVersion();
    }

    /**
     * Initialisation occurs here
     *
     * @throws StorageException if any errors occur
     */
    @Override
    public void shutdown() throws StorageException {
        // Don't need to do anything on shutdown
    }

    /**
     * Retrieve the details for this plugin
     *
     * @return PluginDescription a description of this plugin
     */
    @Override
    public PluginDescription getPluginDetails() {
        return new PluginDescription(this);
    }

    /**
     * Create a new object in storage. An object identifier may be provided, or
     * a null value will try to have Fedora auto-generate the new OID.
     *
     * @param oid the Object ID to use during creation, null is allowed
     * @return DigitalObject the instantiated DigitalObject created
     * @throws StorageException if any errors occur
     */
    @Override
    public synchronized DigitalObject createObject(String oid)
            throws StorageException {
        FedoraClient fedoraClient = null;

        if (oid == null) {
            throw new StorageException("Error; Null OID recieved");
        }
        String fedoraPid = safeFedoraPid(oid);
        String data = null;

        // Can we see object?
        try {
            fedoraClient = Fedora36.getNCClient();
            FedoraResponse response = FedoraClient.getObjectXML(fedoraPid)
                    .execute(fedoraClient);

            // If response code is not in the 200 range check the response
            if (response.getStatus() - 200 < 100) {
                data = response.getEntity(String.class);

                if (data != null && data.getBytes().length > 0) {
                    throw new StorageException("Error; object '" + oid
                            + "' already exists in Fedora");
                }
            }
        } catch (FedoraClientException ex) {
            // Object doesn't exist continue
        } finally {
            Fedora36.releaseNCClient();
        }

        // New content
        try {
            fedoraClient = Fedora36.getNCClient();
            data = new String(prepareTemplate(fedoraPid, oid), "utf-8");
            IngestResponse response = FedoraClient.ingest().content(data)
                    .format(FOXML_VERSION).logMessage(ADD_LOG_MESSAGE)
                    .execute(fedoraClient);

            String responsePid = response.getPid();
            if (!fedoraPid.equals(responsePid)) {
                log.error("Error; PID Mismatch during creation. We sent '{}'"
                        + " but Fedora used '{}'", fedoraPid, responsePid);
                removeFedoraObject(responsePid);
                throw new StorageException("Error with Fedora PIDs. Please"
                        + " check your system logs and configuration!");
            }
            Fedora36.releaseNCClient();
            // Instantiate and return
            return new Fedora36DigitalObject(oid, fedoraPid);
        } catch (Exception ex) {
            throw new StorageException("Error during Fedora search", ex);
        } finally {
            Fedora36.releaseNCClient();
        }
    }

    /**
     * Get the indicated object from storage.
     *
     * @param oid the Object ID to retrieve
     * @return DigitalObject the instantiated DigitalObject requested
     * @throws StorageException if any errors occur
     */
    @Override
    public DigitalObject getObject(String oid) throws StorageException {
        // log.debug("getObject({})", oid);
        if (oid == null) {
            throw new StorageException("Error; Null OID recieved");
        }
        String fedoraPid = safeFedoraPid(oid);
        try {
            FedoraClient fedoraClient = Fedora36.getClient();
            FedoraResponse response = FedoraClient.getObjectXML(fedoraPid)
                    .execute(fedoraClient);
            String data = response.getEntity(String.class);
            // Confirm we can see the object in Fedora
            // byte[] data = Fedora3.getApiM().getObjectXML(fedoraPid);
            if (data == null || data.getBytes().length == 0) {
                throw new StorageException("Error; could not find object '"
                        + oid + "' in Fedora");
            }
            // Instantiate and return
            return new Fedora36DigitalObject(oid, fedoraPid);
        } catch (Exception ex) {
            throw new StorageException("Error accessing Fedora", ex);
        }
    }

    /**
     * Remove the indicated object from storage.
     *
     * @param oid the Object ID to remove from storage
     * @throws StorageException if any errors occur
     */
    @Override
    public synchronized void removeObject(String oid) throws StorageException {
        if (oid == null) {
            throw new StorageException("Error; Null OID recieved");
        }
        String fedoraPid = safeFedoraPid(oid);
        removeFedoraObject(fedoraPid);
    }

    /**
     * Perform the actual removal from Fedora
     *
     * @param fedoraPid the Fedora PID to remove from storage
     * @throws StorageException if any errors occur
     */
    private void removeFedoraObject(String fedoraPid) throws StorageException {
        try {
            FedoraClient fedoraClient = Fedora36.getNCClient();
            FedoraClient.purgeObject(fedoraPid).logMessage(DELETE_LOG_MESSAGE)
            .execute(fedoraClient);
        } catch (Exception ex) {
            throw new StorageException("Error during Fedora search", ex);
        } finally {
            Fedora36.releaseNCClient();
        }
    }

    /**
     * Return a list of Object IDs currently in storage.
     *
     * @return Set<String> A Set containing all the OIDs in storage.
     */
    @Override
    public Set<String> getObjectIdList() {
        log.info("Complete storage OID list requested...");
        Set<String> objectList = new HashSet<String>();
        try {
            FedoraClient fedoraClient = Fedora36.getClient();
            FindObjectsResponse response = FedoraClient.findObjects()
                    .terms(Fedora36.namespace() + ":*")
                    .maxResults(SEARCH_ROW_LIMIT_PER_PAGE)
                    .execute(fedoraClient);

            if (response.getStatus() == 200) {
                while (true) {
                    for (String pid : response.getPids()) {
                        objectList.add(response.getObjectField(pid, "label")
                                .get(0));
                    }
                    if (!response.hasNext()) {
                        break;
                    }
                    response = FedoraClient.findObjects()
                            .sessionToken(response.getToken())
                            .execute(fedoraClient);
                }
            }
        } catch (Exception e) {
            log.error("Error during Fedora search: ", e);
            return null;
        }
        return objectList;
    }

    /**
     * Translate a Fascinator OID into a hashed Fedora ID with namespace. Should
     * prevent any issues related to special characters being used in IDs
     *
     * @param oid the Object ID from Fascinator
     * @return String the Fedora PID to use
     */
    private String safeFedoraPid(String oid) {
        return Fedora36.namespace() + ":" + DigestUtils.md5Hex(oid);
    }

    /**
     * Access a resource by name and stream the contents into a temp file.
     *
     * @param resourceName The name of the resource (assumed to be on base path)
     * @return File a temporary File containing the resource's data
     */
    private File resourceToFile(String resourcesName) throws IOException {
        InputStream in = getClass().getResourceAsStream("/" + resourcesName);
        File newFile = File.createTempFile("tempResource", "tmp");
        FileOutputStream out = new FileOutputStream(newFile);
        IOUtils.copy(in, out);
        in.close();
        out.close();
        return newFile;
    }

    /**
     * Reads a File into a String.
     *
     * @param file the File to read
     * @return String the Fedora PID to use
     */
    private String fileToString(File file) {
        // Prepare
        int expectedLength = (int) file.length();
        byte[] buffer = new byte[expectedLength];
        BufferedInputStream inputStream = null;
        try {
            // Perform
            inputStream = new BufferedInputStream(new FileInputStream(file));
            int bytesRead = inputStream.read(buffer);
            // Validate
            if (bytesRead != expectedLength) {
                log.error(
                        "Error reading file data; {} bytes read; expected {}",
                        bytesRead, expectedLength);
                return null;
            }

        } catch (Exception ex) {
            log.error("Error accessing file '{}': ", file.getName(), ex);
            return null;
        } finally {
            Fedora36.close(inputStream);
        }
        return new String(buffer);
    }

    /**
     * Prepare a FOXML Template for Fedora, including the provided PID.
     *
     * @param pid The desired Fedora PID
     * @return byte[] The evaluated template as a byte array to send to Fedora
     */
    private byte[] prepareTemplate(String pid, String oid) {
        String output = foxmlTemplate.replace("[[PID]]", pid);
        output = output.replace("[[OID]]", oid);
        try {
            return output.getBytes("UTF-8");
        } catch (Exception ex) {
            log.error("Encoding error in template: ", ex);
            return null;
        }
    }
}
