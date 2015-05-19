/*
 * The Fascinator - Plugin - Storage - Fedora 3
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

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;

@SuppressWarnings("unused")
@Ignore
public class Fedora3StorageTest {
    private Fedora36Storage storage;
    private String fedoraVersion;

    @Before
    public void init() throws Exception {
        storage = new Fedora36Storage();
        storage.init(new File(getClass().getResource("/fedora3-config.json")
                .toURI()));
        fedoraVersion = storage.fedoraVersion();
        // Make sure it always starts empty
        sizeTest(0);
    }

    @After
    public void cleanup() throws Exception {
        if (storage != null) {
            storage.shutdown();
        }
        // Super Nuke... We expect 'messy' failures to cause a size test to
        // fail in the init() method, but cleaning up for the next test can
        // be annoying if done manually. Uncomment this for one execution.
        // nukeStorage();
    }

    private void nukeStorage() throws Exception {
        for (String oid : storage.getObjectIdList()) {
            storage.removeObject(oid);
        }
    }

    /**
     * Test the object creation process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void objectCreation() throws Exception {
        System.out.println("\n==========\n TEST => objectCreation()\n");
        // 1) Null OID
        try {
            storage.createObject(null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Normal creation
        storage.createObject("testObject1");
        sizeTest(1);

        // 3) Duplicate creation
        try {
            storage.createObject("testObject1");
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }
        sizeTest(1);

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Test the object retrieval process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void objectRetrieval() throws Exception {
        System.out.println("\n==========\n TEST => objectRetrieval()\n");
        // 1) Null OID
        try {
            storage.getObject(null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Does not exist
        try {
            storage.getObject("testObject1");
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 3) Normal retrieval... after creation
        storage.createObject("testObject1");
        sizeTest(1);
        DigitalObject object = storage.getObject("testObject1");
        Assert.assertNotNull(object);
        Assert.assertEquals("Object ID does not match", "testObject1",
                object.getId());

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Test the object removal process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void objectRemoval() throws Exception {
        System.out.println("\n==========\n TEST => objectRemoval()\n");
        // 1) Null OID
        try {
            storage.removeObject(null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Does not exist
        try {
            storage.removeObject("testObject1");
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 3) Normal removal... after creation
        storage.createObject("testObject1");
        sizeTest(1);
        storage.removeObject("testObject1");
        sizeTest(0);
    }

    /**
     * Test the payload creation process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void payloadCreation() throws Exception {
        System.out.println("\n==========\n TEST => payloadCreation()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) Null - Both
        try {
            object.createStoredPayload(null, null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Null - PID
        try {
            object.createStoredPayload(null, in("testPayload1.txt"));
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 3) Null - Stream
        try {
            object.createStoredPayload("testPayload1", null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 4) Normal creation
        sizeTest(1);
        sizeTest(object, 0);
        object.createStoredPayload("testPayload1", in("testPayload1.txt"));
        sizeTest(1);
        sizeTest(object, 1);
        object.createStoredPayload("testPayload2", in("testPayload2.xml"));
        sizeTest(1);
        sizeTest(object, 2);

        // 5) Duplicate creation
        try {
            object.createStoredPayload("testPayload1", in("testPayload1.txt"));
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }
        sizeTest(1);
        sizeTest(object, 2);

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Test the payload storage process, related to creation above, but very
     * specific
     * 
     * @throws Exception
     */
    @Test
    public void payloadStorage() throws Exception {
        System.out.println("\n==========\n TEST => payloadStorage()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) 'Stored' payload - 16261 bytes
        sizeTest(1);
        sizeTest(object, 0);
        Payload payload1 = object.createStoredPayload("testPayload1",
                in("testPayload4.png"));
        sizeTest(1);
        sizeTest(object, 1);
        sizeTest(payload1, 16261);
        Assert.assertEquals(false, payload1.isLinked());

        // 2) 'Linked' payload - 16261 bytes... ie. Not really linked
        Payload payload2 = object.createLinkedPayload("testPayload2",
                path("testPayload4.png"));
        sizeTest(1);
        sizeTest(object, 2);
        sizeTest(payload2, 16261);
        Assert.assertEquals(false, payload1.isLinked());

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Test the payload retrieval process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void payloadRetrieval() throws Exception {
        System.out.println("\n==========\n TEST => payloadRetrieval()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) Null PID
        try {
            object.getPayload(null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Does not exist
        try {
            object.getPayload("testPayload1");
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 3) Normal retrieval... after creation
        sizeTest(1);
        sizeTest(object, 0);
        object.createStoredPayload("testPayload1", in("testPayload1.txt"));
        sizeTest(1);
        sizeTest(object, 1);
        Payload payload = object.getPayload("testPayload1");
        Assert.assertNotNull(payload);
        Assert.assertEquals("Payload ID does not match", "testPayload1",
                payload.getId());

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Test the payload removal process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void payloadRemoval() throws Exception {
        System.out.println("\n==========\n TEST => payloadRemoval()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) Null PID
        try {
            object.removePayload(null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Does not exist
        try {
            object.removePayload("testPayload1");
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 3) Normal removal... after creation
        object.createStoredPayload("testPayload1", in("testPayload1.txt"));
        sizeTest(object, 1);
        object.removePayload("testPayload1");
        sizeTest(1);
        sizeTest(object, 0);
        storage.removeObject("testObject1");
        sizeTest(0);
    }

    /**
     * Test the payload update process for all use cases
     * 
     * @throws Exception
     */
    @Test
    public void payloadUpdate() throws Exception {
        System.out.println("\n==========\n TEST => payloadUpdate()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) Null - Both
        try {
            object.updatePayload(null, null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 2) Null - PID
        try {
            object.updatePayload(null, in("testPayload1.txt"));
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 3) Null - Stream
        try {
            object.updatePayload("testPayload1", null);
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 4) Does not exist
        try {
            object.updatePayload("testPayload1", in("testPayload1.txt"));
            Assert.fail();
        } catch (StorageException ex) {
            // This is what should occur
        }

        // 5) Normal update... after creation
        Payload payload1 = object.createStoredPayload("testPayload1",
                in("testPayload4.png"));
        sizeTest(1);
        sizeTest(object, 1);
        sizeTest(payload1, 16261);
        Assert.assertEquals("image/png", payload1.getContentType());
        Assert.assertEquals("Source", payload1.getType().toString());
        Long lastMod = payload1.lastModified();

        // The replacement will be much smaller, and a new MIME type
        payload1 = object.updatePayload("testPayload1", in("testPayload1.txt"));
        sizeTest(1);
        sizeTest(object, 1);
        sizeTest(payload1, 4);
        Assert.assertEquals("text/plain", payload1.getContentType());
        Assert.assertEquals("Source", payload1.getType().toString());
        Assert.assertNotSame(lastMod, payload1.lastModified());

        storage.removeObject("testObject1");
        sizeTest(0);
    }

    /**
     * Test the payload open/close cycle and connection/stream access
     * 
     * @throws Exception
     */
    @Test
    public void payloadOpenClose() throws Exception {
        System.out.println("\n==========\n TEST => payloadOpenClose()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");
        Payload payload1 = object.createStoredPayload("testPayload1",
                in("testPayload1.txt"));
        Payload payload2 = object.createStoredPayload("testPayload2",
                in("testPayload1.txt"));
        Payload payload3 = object.createStoredPayload("testPayload3",
                in("testPayload1.txt"));
        sizeTest(1);
        sizeTest(object, 3);

        // TODO: This test would be nice, but not currently possible. See notes
        // in Fedora3.release() for the limitations of the current API.
        // 1) Reading a closed stream
        // try {
        // InputStream in = payload1.open();
        // payload1.close();
        // in.read();
        // Assert.fail();
        // } catch (IOException ex) {
        // This is what should occur
        // }

        // 2) Massed connection closures... tests that the connection caching
        // in not holding lots of open connections IF they are being closed
        // cleanly. This test is only really useful on Windows, since other
        // OS's will probably take the abuse. Windows will exceed connection
        // license limits for no-server editions.
        // * Other OS's may notice memory issues related to the cache however,
        // Uncomment and make the limit as extreme as desired...

        int limit = 10;
        /*
        // The list is outside the loop so connections aren't dereference and
        //  end up in garbage collection before we've finished.
        List<InputStream> list = new ArrayList();
        for (int i = 0; i < limit; i++) {
            list.add(payload1.open());
        }
        // Touch each stream once
        for (InputStream in : list) {
            in.read(); // Read some data... in case it matters
        }
        // Sleep for a bit
        Thread.sleep(10000);
        // Then come back to them
        for (InputStream in : list) {
            // Comment different lines here to try and trigger failures
            in.read(); // Read some more data... in case it matters
            in.close(); // Release cache won't work without this
            payload1.close(); // Release cache here
        }
        // Purge objects for garbage collection
        list.clear();
        // */

        // 3) Trivial read of actual data
        InputStream in = payload1.open();
        Assert.assertEquals(84, in.read()); // 'T'
        Assert.assertEquals(101, in.read()); // 'e'
        Assert.assertEquals(115, in.read()); // 's'
        Assert.assertEquals(116, in.read()); // 't'
        in.close();
        payload1.close();

        storage.removeObject("testObject1");
        sizeTest(0);
    }

    /**
     * Test all Payload methods
     * 
     * @throws Exception
     */
    @Test
    public void payloadCompleteTest() throws Exception {
        System.out.println("\n==========\n TEST => payloadCompleteTest()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) Store a payload
        sizeTest(1);
        sizeTest(object, 0);
        Payload payload1 = object.createStoredPayload("testPayload1",
                in("testPayload4.png"));
        sizeTest(1);
        sizeTest(object, 1);
        sizeTest(payload1, 16261);

        // 2) Test properties
        Assert.assertEquals("testPayload1", object.getSourceId());
        Assert.assertEquals("image/png", payload1.getContentType());
        Assert.assertEquals("testPayload1", payload1.getId());
        Assert.assertEquals("testPayload1", payload1.getLabel());
        Assert.assertEquals("Source", payload1.getType().toString());
        Assert.assertEquals(false, payload1.isLinked());
        sizeTest(payload1, 16261);

        // 3) Manual alterations... without closing
        payload1.setContentType("random/invalid");
        payload1.setLabel("Test Payload 1");
        Payload payload2 = object.getPayload("testPayload1");
        Assert.assertEquals("testPayload1", payload2.getId());
        Assert.assertEquals("testPayload1", payload2.getLabel());
        Assert.assertEquals("image/png", payload2.getContentType());

        // 4) Now close and test again
        payload1.close();
        payload2 = object.getPayload("testPayload1");
        Assert.assertEquals("testPayload1", payload2.getId());
        Assert.assertEquals("Test Payload 1", payload2.getLabel());
        Assert.assertEquals("random/invalid", payload2.getContentType());

        // 4) Type alterations
        // This is an invalid object now, we just altered the type on the source
        payload1.setType(PayloadType.Enrichment);
        // Pre-save
        payload2 = object.getPayload("testPayload1");
        Assert.assertEquals("Source", payload2.getType().toString());
        Assert.assertEquals("testPayload1", object.getSourceId());
        // And save...
        payload1.close();
        payload2 = object.getPayload("testPayload1");
        Assert.assertEquals("Enrichment", payload2.getType().toString());
        // Is the source up-to-date now? We know it is invalid
        Assert.assertEquals("testPayload1", object.getSourceId());
        object.close();
        object = storage.getObject("testObject1");
        Assert.assertNull(object.getSourceId());

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Mostly testing modification times, but unlike the payloadUpdate() method
     * we are also checking persistence from two object references.
     * 
     * @throws Exception
     */
    @Test
    public void payloadModify() throws Exception {
        System.out.println("\n==========\n TEST => payloadModify()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // 1) Store a payload
        sizeTest(1);
        sizeTest(object, 0);
        Payload payload1 = object.createStoredPayload("testPayload1",
                in("testPayload1.txt"));
        sizeTest(1);
        sizeTest(object, 1);

        // 2) Closed but not changes
        Long startTime = payload1.lastModified();
        payload1.close();
        payload1 = object.getPayload("testPayload1");
        Long nowTime = payload1.lastModified();
        Assert.assertEquals(startTime, nowTime);

        // 3) Metadata changed
        payload1.setLabel("Test Payload 1");
        payload1.close();
        payload1 = object.getPayload("testPayload1");
        nowTime = payload1.lastModified();
        Assert.assertTrue(startTime < nowTime);

        // 4) Data changed
        object.updatePayload("testPayload1", in("testPayload2.xml"));
        payload1 = object.getPayload("testPayload1");
        Long newTime = payload1.lastModified();
        Assert.assertTrue(nowTime < newTime);

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Modify the metadata on an object and ensure it persists.
     * 
     * @throws Exception
     */
    @Test
    public void metadataTest() throws Exception {
        System.out.println("\n==========\n TEST => metadataTest()\n");
        // Create a basic test object
        DigitalObject object = storage.createObject("testObject1");

        // Alter the object's metadata
        Properties metadata = object.getMetadata();
        metadata.setProperty("testProperty1", "set");
        object.close();

        // Re-instantiate and check
        DigitalObject object2 = storage.getObject("testObject1");
        Properties metadata2 = object2.getMetadata();
        Assert.assertEquals("set", metadata2.getProperty("testProperty1"));

        // Cleanup
        storage.removeObject("testObject1");
    }

    /**
     * Bash away at storage with a significant object and payload usage load.
     * Run a slew of assertions against each object afterwards.
     * 
     * Similar to the connection testing above, set the limit as high as you
     * would like to stress test your system.
     * 
     * @throws Exception
     */
    @Test
    public void stressTest() throws Exception {
        System.out.println("\n==========\n TEST => stressTest()\n");
        int objectLimit = 10;

        for (int i = 0; i < objectLimit; i++) {
            DigitalObject object = storage.createObject("testObject" + i);
            // Payload 1
            Payload payload1 = object.createStoredPayload("testPayload1",
                    in("testPayload1.txt"));
            Assert.assertEquals("testPayload1", object.getSourceId());
            payload1.setType(PayloadType.Preview);
            payload1.setLabel("Test Payload 1");
            payload1.close();
            // Payload 2
            Payload payload2 = object.createStoredPayload("testPayload2",
                    in("testPayload2.xml"));
            payload2.setType(PayloadType.Annotation);
            payload2.setLabel("Test Payload 2 from Object " + i);
            payload2.close();
            // Payload 3
            Payload payload3 = object.createStoredPayload("testPayload3",
                    in("testPayload3"));
            payload3.setType(PayloadType.Error);
            payload3.close();
            // Payload 4
            Payload payload4 = object.createStoredPayload("testPayload4",
                    in("testPayload4.png"));
            payload4.setType(PayloadType.Source);
            payload4.close();

            // Metadata payload
            Properties metadata = object.getMetadata();
            metadata.setProperty("objectId", "testObject" + i);

            // Confirm expected sizes
            sizeTest(i + 1);
            sizeTest(object, 5);
            object.close();
        }

        // Reset our storage object for genuine persistence testing
        storage.shutdown();
        storage = null;
        storage = new Fedora36Storage();
        storage.init(new File(getClass().getResource("/fedora3-config.json")
                .toURI()));

        // Testing
        for (int i = 0; i < objectLimit; i++) {
            DigitalObject object = storage.getObject("testObject" + i);
            sizeTest(object, 5);
            Assert.assertEquals("testPayload4", object.getSourceId());

            Payload payload = object.getPayload("testPayload1");
            Assert.assertEquals("testPayload1", payload.getId());
            Assert.assertEquals("text/plain", payload.getContentType());
            Assert.assertEquals("Test Payload 1", payload.getLabel());
            Assert.assertEquals("Preview", payload.getType().toString());

            payload = object.getPayload("testPayload2");
            Assert.assertEquals("testPayload2", payload.getId());
            Assert.assertEquals("text/xml", payload.getContentType());
            Assert.assertEquals("Test Payload 2 from Object " + i,
                    payload.getLabel());
            Assert.assertEquals("Annotation", payload.getType().toString());

            payload = object.getPayload("testPayload3");
            Assert.assertEquals("testPayload3", payload.getId());
            Assert.assertEquals("text/plain", payload.getContentType());
            Assert.assertEquals("testPayload3", payload.getLabel());
            Assert.assertEquals("Error", payload.getType().toString());

            payload = object.getPayload("testPayload4");
            Assert.assertEquals("testPayload4", payload.getId());
            Assert.assertEquals("image/png", payload.getContentType());
            Assert.assertEquals("testPayload4", payload.getLabel());
            Assert.assertEquals("Source", payload.getType().toString());

            // Metadata
            Properties metadata = object.getMetadata();
            Assert.assertEquals("testObject" + i,
                    metadata.getProperty("objectId"));
        }

        // Cleanup
        for (int i = 0; i < objectLimit; i++) {
            DigitalObject object = storage.getObject("testObject" + i);
            sizeTest(object, 5);
            object.removePayload("testPayload1");
            sizeTest(object, 4);
            object.removePayload("testPayload2");
            sizeTest(object, 3);
            object.removePayload("testPayload3");
            sizeTest(object, 2);
            object.removePayload("testPayload4");
            sizeTest(object, 1);
            storage.removeObject("testObject" + i);
            sizeTest(objectLimit - (i + 1));
        }
    }

    /**
     * This test doesn't really do anything, but running it last does confirm
     * that all previous tests cleaned up properly.
     * 
     * @throws Exception
     */
    @Test
    public void emptyTest() throws Exception {
        sizeTest(0);
    }

    /**
     * Wrapper for resource retrieval as InputStreams, simply to reduce call
     * complexity to just 'in("file.name")',
     * 
     * @throws Exception
     */
    private InputStream in(String fileName) {
        return getClass().getResourceAsStream("/" + fileName);
    }

    /**
     * Wrapper for resource retrieval as String paths, simply to reduce call
     * complexity to just 'in("file.name")',
     * 
     * @throws Exception
     */
    private String path(String fileName) throws Exception {
        // Annoyingly enough, a straight toString() call doesn't work
        // 1) Create the File object with a URI
        File file = new File(getClass().getResource("/" + fileName).toURI());
        // 2) Return a valid File path via the File class
        return file.getAbsolutePath();
    }

    /**
     * Confirm that the number of objects in Storage is as expected. This method
     * simply wraps up the storage request, JUnit assertion and common message
     * wording on fails.
     * 
     * @param number The expected number of objects to be found in storage
     */
    private void sizeTest(int number) {
        Assert.assertEquals("There should be " + number
                + " object(s) in storage!", number, storage.getObjectIdList()
                .size());
    }

    /**
     * Confirm that the number of payloads in an object is as expected. This
     * method simply wraps up the storage request, JUnit assertion and common
     * message wording on fails.
     * 
     * @param object The DigitalObject being tested
     * @param number The expected number of payloads to be found in the object
     */
    private void sizeTest(DigitalObject object, int number) {
        Assert.assertEquals("There should be " + number
                + " payload(s) in object!", number, object.getPayloadIdList()
                .size());
    }

    /**
     * Confirm that the size of payload data is expected. This method wraps up
     * the storage request, JUnit assertion and common message wording on
     * failure, as well as checking for shortcomings in earlier versions of
     * Fedora which will make this feature unsupportable.
     * 
     * @param payload The Payload being tested
     * @param number The expected byte size of data to find in the Payload
     */
    private void sizeTest(Payload payload, int number) {
        // Pre-v3.4 does not support payload size unless XML is in datastream
        if (fedoraVersion.startsWith("3.3")) {
            number = 0;
        }
        Assert.assertEquals("Payload should be " + number + " bytes in size!",
                Long.valueOf(number), payload.size());
    }
}
