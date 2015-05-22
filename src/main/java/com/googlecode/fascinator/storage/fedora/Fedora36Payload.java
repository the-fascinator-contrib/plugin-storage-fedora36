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

import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.MimeTypeUtil;
import com.googlecode.fascinator.common.storage.impl.GenericPayload;
import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.request.FedoraRequest;
import com.yourmediashelf.fedora.client.request.GetDatastream;
import com.yourmediashelf.fedora.client.request.GetDatastreamHistory;
import com.yourmediashelf.fedora.client.request.ModifyDatastream;
import com.yourmediashelf.fedora.client.response.DatastreamProfileResponse;
import com.yourmediashelf.fedora.client.response.FedoraResponse;
import com.yourmediashelf.fedora.client.response.GetDatastreamHistoryResponse;
import com.yourmediashelf.fedora.client.response.GetDatastreamResponse;
import com.yourmediashelf.fedora.generated.management.DatastreamProfile;

/**
 * Maps a Fedora datastream to a Fascinator payload.
 *
 * @author Oliver Lucido
 * @author Greg Pendlebury
 */
public class Fedora36Payload extends GenericPayload {
	/* Fedora log message for updating metadata */
	private static String METADATA_LOG_MESSAGE = "Fedora3Payload metadata updated";

	/** Logging */
	private Logger log = LoggerFactory.getLogger(Fedora36Payload.class);

	/** Date parsing */
	private SimpleDateFormat dateParser = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	/** Fedora PID */
	private String fedoraPid;

	/** Fedora DSID */
	private String dsId;

	/**
	 * Instantiate a brand new payload in Fedora
	 *
	 * @param pid
	 *            the Fascinator Payload ID
	 * @param fedoraPid
	 *            the Object OID in Fedora (PID) for this datastream
	 * @param dsId
	 *            the datastream ID in Fedora
	 */
	public Fedora36Payload(String pid, String fedoraPid, String dsId) {
		super(pid, pid, MimeTypeUtil.DEFAULT_MIME_TYPE);
		// log.debug("Construct NEW({},{},{})",
		// new String[] {pid, fedoraPid, dsId});
		init(fedoraPid, dsId);
	}

	/**
	 * Instantiate an existing payload from Fedora
	 *
	 * @param dsProfile
	 *            contains Fedora Datastream information
	 * @param pid
	 *            the Fascinator Payload ID
	 * @param fedoraPid
	 *            the Object OID in Fedora (PID) for this datastream
	 * @param dsId
	 *            the datastream ID in Fedora
	 */
	public Fedora36Payload(DatastreamProfile ds, String pid, String fedoraPid) {
		super(pid, ds.getDsLabel(), ds.getDsMIME(), PayloadType.valueOf(ds
				.getDsAltID().get(0)));
		// log.debug("Construct EXISTING ({},{},{})",
		// new String[] {pid, fedoraPid, ds.getID()});
		init(fedoraPid, ds.getDsID());
	}

	private void init(String fedoraPid, String dsId) {
		this.fedoraPid = fedoraPid;
		this.dsId = dsId;
	}

	/**
	 * Gets the input stream to access the content for this payload
	 *
	 * @return an input stream
	 * @throws IOException
	 *             if there was an error reading the stream
	 */
	@Override
	public InputStream open() throws StorageException {
		// log.debug("open({})", getId());
		close();

		try {
			return Fedora36.getStream(fedoraPid, dsId);
		} catch (RemoteException ex) {
			log.error("Error during Fedora search: ", ex);
			return null;
		} catch (IOException ex) {
			log.error("Error accessing Fedora: ", ex);
			return null;
		}
	}

	/**
	 * Close the input stream for this payload
	 *
	 * @throws StorageException
	 *             if there was an error closing the stream
	 */
	@Override
	public void close() throws StorageException {
		// log.debug("close({})", getId());
		Fedora36.release(fedoraPid, dsId);
		if (hasMetaChanged()) {
			updateMeta();
		}
	}

	/**
	 * Update payload metadata
	 *
	 * @throws StorageException
	 *             if there was an error
	 */
	private void updateMeta() throws StorageException {
		// log.debug("updateMeta({})", getId());

		PayloadType type = getType();
		if (type == null) {
			type = PayloadType.Enrichment;
		}
		try {
			// NULL values indicate we aren't changing that parameter
			String[] altIds = new String[] { getType().toString(), getId() };
			FedoraClient fedoraClient = Fedora36.getNCClient();
			ModifyDatastream modifyDatastream = FedoraClient
					.modifyDatastream(fedoraPid, dsId)
					.altIDs(Arrays.asList(altIds)).dsLabel(getLabel())
					.mimeType(getContentType()).dsLocation(null)
					.logMessage(METADATA_LOG_MESSAGE).versionable(false);

			executeFedoraRequest(fedoraClient, modifyDatastream);
			setMetaChanged(false);
		} catch (Exception ioe) {
			throw new StorageException(ioe);
		} finally {
			Fedora36.releaseNCClient();
		}
	}

	/**
	 * Return the timestamp when the payload was last modified
	 *
	 * @returns Long: The last modified date of the payload, or NULL if unknown
	 */
	@Override
	public Long lastModified() {
		// log.debug("lastModified({})", getId());
		try {
			// Grab the history of this object's payloads
			FedoraClient fedoraClient = Fedora36.getNCClient();
			GetDatastreamHistory getDatastreamHistory = FedoraClient
					.getDatastreamHistory(fedoraPid, dsId);
			List<DatastreamProfile> datastreams = ((GetDatastreamHistoryResponse) executeFedoraRequest(
					fedoraClient, getDatastreamHistory)).getDatastreamProfile()
					.getDatastreamProfile();

			if (datastreams == null || datastreams.size() == 0) {
				log.error("Error accessing datastream history: '{}' DS '{}'",
						fedoraPid, dsId);
				return null;
			}
			Date lastModified = datastreams.get(0).getDsCreateDate()
					.toGregorianCalendar().getTime();
			return lastModified.getTime();
		} catch (Exception ex) {
			log.error("Error in Fedora query: ", ex);
			return null;
		} finally {
			Fedora36.releaseNCClient();
		}
	}

	/**
	 * Return the size of the payload in bytes
	 *
	 * @returns Integer: The file size in bytes, or NULL if unknown
	 */
	@Override
	public Long size() {
		try {
			FedoraClient fedoraClient = Fedora36.getNCClient();
			GetDatastream getDatastream = FedoraClient.getDatastream(fedoraPid,
					dsId);
			DatastreamProfile datastream = ((DatastreamProfileResponse) executeFedoraRequest(
					fedoraClient, getDatastream)).getDatastreamProfile();

			return datastream.getDsSize().longValue();
		} catch (Exception ex) {
			log.error("Error in Fedora query: ", ex);
			return null;
		} finally {
			Fedora36.releaseNCClient();
		}
	}

	private FedoraResponse executeFedoraRequest(FedoraClient fedoraClient,
			FedoraRequest<?> fedoraRequest) throws FedoraClientException {
		return fedoraRequest.execute(fedoraClient);

	}
}
