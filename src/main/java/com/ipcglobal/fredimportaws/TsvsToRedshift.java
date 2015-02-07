/**
 *    Copyright 2015 IPC Global (http://www.ipc-global.com) and others.
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.ipcglobal.fredimportaws;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import com.ipcglobal.fredimport.util.FredUtils;
import com.ipcglobal.fredimport.util.LogTool;


/**
 * The Class TsvsToRedshift.
 */
public class TsvsToRedshift {
	
	/** The Constant log. */
	private static final Log log = LogFactory.getLog(TsvsToRedshift.class);
	
	/** The Constant dfCommas. */
	private static final DecimalFormat dfCommas = new DecimalFormat( "#,##0" );
	
	/** The Constant dfDec3. */
	private static final DecimalFormat dfDec3 = new DecimalFormat( "#,##0.000" );
	
	/** The Constant megs. */
	private static final double megs = 1024*1024;
	
	/** The credentials provider. */
	private AWSCredentialsProvider credentialsProvider;
	
	/** The s3 client. */
	private AmazonS3Client s3Client;
	
	/** The transfer manager. */
	private TransferManager transferManager;
	
	/** The sts client. */
	private AWSSecurityTokenServiceClient stsClient;

	/** The properties. */
	private Properties properties;
	
	/** The aws bucket name. */
	private String awsBucketName;
	
	/** The aws bucket tsv prefix. */
	private String awsBucketTsvPrefix;
	
	/** The path table tsv files. */
	private String pathTableTsvFiles;

	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		LogTool.initConsole();
		if( args.length < 1 ) {
			log.error("Properties path/name is required");
			System.exit(8);
		}
		
		LogTool.initConsole();
		TsvsToRedshift tsvsToRedshift = null;
		try {
			tsvsToRedshift = new TsvsToRedshift( args[0] );
			tsvsToRedshift.process();

		} catch (Exception e) {
			log.error(e);
			e.printStackTrace();
		}
	}

	/**
	 * Instantiates a new tsvs to redshift.
	 *
	 * @param pathNameProperties the path name properties
	 * @throws Exception the exception
	 */
	public TsvsToRedshift( String pathNameProperties ) throws Exception {
		this.properties = new Properties( );
		properties.load(new FileInputStream( pathNameProperties ) );
		String credentialsProfileName = this.properties.getProperty("credentialsProfileName").trim();
		this.awsBucketName = this.properties.getProperty("awsBucketName").trim();
		this.awsBucketTsvPrefix = this.properties.getProperty("awsBucketTsvPrefix").trim();
		
		String outputPath = FredUtils.readfixPath( "outputPath", properties );		
		String outputSubdirTableTsvFiles = FredUtils.readfixPath( "outputSubdirTableTsvFiles", properties );
		this.pathTableTsvFiles = outputPath + outputSubdirTableTsvFiles;
		
		if( credentialsProfileName == null ) this.credentialsProvider = AwsUtils.initCredentials();
		else this.credentialsProvider = AwsUtils.initProfileCredentialsProvider( credentialsProfileName );

		this.s3Client = new AmazonS3Client( credentialsProvider );
		this.transferManager = new TransferManager( credentialsProvider );
		this.stsClient = new AWSSecurityTokenServiceClient( credentialsProvider );
	}

	
	/**
	 * Process.
	 *
	 * @throws Exception the exception
	 */
	public void process( ) throws Exception {
		try {
			s3Client.createBucket(new CreateBucketRequest(awsBucketName));
			log.info("Start: emptyBucketContents");
			long before = System.currentTimeMillis();
			emptyBucketContents();
			log.info("Complete: emptyBucketContents, elapsed=" + (System.currentTimeMillis()-before ));
			
			log.info("Start: transferToBucket");
			before = System.currentTimeMillis();
			transferToBucket();
			log.info("Complete: transferToBucket, elapsed=" + (System.currentTimeMillis()-before ));
			
			log.info("Start: copyS3FilesToRedshiftTable");
			before = System.currentTimeMillis();
			copyS3FilesToRedshiftTable();
			log.info("Complete: copyS3FilesToRedshiftTable, elapsed=" + (System.currentTimeMillis()-before ));
	
		} catch (AmazonServiceException ase) {
			log.error("Caught Exception: " + ase.getMessage());
			log.error("Reponse Status Code: " + ase.getStatusCode());
			log.error("Error Code: " + ase.getErrorCode());
			log.error("Request ID: " + ase.getRequestId());
			throw ase;
		} catch (AmazonClientException ace) {
			log.error("Error Message: " + ace.getMessage());
			throw ace;
		} catch( Exception e ) {
			log.error(e);
			throw e;
		}
	}
		
	
	/**
	 * Transfer to bucket.
	 *
	 * @throws Exception the exception
	 */
	private void transferToBucket() throws Exception {
		long before = System.currentTimeMillis();
		MultipleFileUpload multipleFileUpload = transferManager.uploadDirectory( awsBucketName, awsBucketTsvPrefix, new File(pathTableTsvFiles), true );
		MultiUploadProgressListener multiUploadProgressListener = new MultiUploadProgressListener();
		multipleFileUpload.addProgressListener(multiUploadProgressListener);			 
		multipleFileUpload.waitForCompletion();
		long elapsedSecs = (System.currentTimeMillis()-before) / 1000 ;
		log.info("ElapsedSecs: " + elapsedSecs + ", TotMBytes=" + 
				dfDec3.format( (multiUploadProgressListener.getTotBytes()/megs) )
				+ ", Rate (Mbytes/sec)=" + dfDec3.format( ( multiUploadProgressListener.getTotBytes()/megs) /elapsedSecs )
				+ ", Rate (Mbytes/min)=" + dfDec3.format( ((multiUploadProgressListener.getTotBytes()/megs) / elapsedSecs) * 60d ) 
				);

		transferManager.shutdownNow();
	}
	

	/**
	 * Copy s3 files to redshift table.
	 *
	 * @throws Exception the exception
	 */
	private void copyS3FilesToRedshiftTable() throws Exception {
    	GetSessionTokenRequest getSessionTokenRequest = new GetSessionTokenRequest();
    	GetSessionTokenResult getSessionTokenResult = stsClient.getSessionToken(getSessionTokenRequest);
    	Credentials credentialsToken = getSessionTokenResult.getCredentials();
		String jdbcRedshiftUrl = properties.getProperty( "jdbcRedshiftUrl" );
		String jdbcRedshiftDriverClass = properties.getProperty( "jdbcRedshiftDriverClass" );
		String jdbcRedshiftLogin = properties.getProperty( "jdbcRedshiftLogin" );
		String jdbcRedshiftPassword = properties.getProperty( "jdbcRedshiftPassword" );

		Class.forName(jdbcRedshiftDriverClass);
		Connection con = null;
		Statement statement = null;
		
		try {
			String tableName = "tbfred";
			con = DriverManager.getConnection( jdbcRedshiftUrl, jdbcRedshiftLogin, jdbcRedshiftPassword);
			statement = con.createStatement();
			// Drop/Create table (more efficient than deleting all of the rows)
			statement.execute( "DROP TABLE " + tableName );
			statement.execute( createTableStatement( tableName ) );

			long beforeCopy = System.currentTimeMillis();
			String s3SourceBucketPrefix = "s3://" + awsBucketName + "/" + awsBucketTsvPrefix + "/";
			String s3Copy = "copy " + tableName + " from '" + s3SourceBucketPrefix + "' " +
					"CREDENTIALS 'aws_access_key_id=" + credentialsToken.getAccessKeyId().replace("\\", "\\\\") + ";" +
					"aws_secret_access_key=" + credentialsToken.getSecretAccessKey().replace("\\", "\\\\") + ";" + 
					"token=" + credentialsToken.getSessionToken().replace("\\", "\\\\") + "' " +
					"delimiter '\\t' gzip";
			statement.executeUpdate(s3Copy);
			
		} catch( Exception e ) {
			log.error(e);
			throw e;
		} finally {
			try {
				if( statement != null && !statement.isClosed() ) statement.close();
			} catch( Exception e ) {
				log.warn("Exception closing statement: " + e.getMessage());
			}

			try {
				if( con != null && !con.isClosed() ) con.close();
			} catch( Exception e ) {
				log.warn("Exception closing connection: " + e.getMessage());
			}			
		}	
	}
	
	/**
	 * Empty bucket contents.
	 *
	 * @throws Exception the exception
	 */
	private void emptyBucketContents( ) throws Exception {
		try {

			while( true ) {
				ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(awsBucketName)
						.withPrefix(awsBucketTsvPrefix+"/").withDelimiter("/"));
				if( objectListing.getObjectSummaries().size() == 0 ) break;
				List<KeyVersion> keyVersions = new ArrayList<KeyVersion>();
				for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) 
					keyVersions.add( new KeyVersion( s3ObjectSummary.getKey() ) );

				if( keyVersions.size() > 0 ) {						
					DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(awsBucketName);
					deleteObjectsRequest.setKeys(keyVersions);
					s3Client.deleteObjects(deleteObjectsRequest);
				}
			}

		} catch (AmazonServiceException ase) {
			log.error(ase);
			throw ase;
		} catch (AmazonClientException ace) {
			log.error(ace);
			throw ace;
		}
	}

	/**
	 * The listener interface for receiving multiUploadProgress events.
	 * The class that is interested in processing a multiUploadProgress
	 * event implements this interface, and the object created
	 * with that class is registered with a component using the
	 * component's <code>addMultiUploadProgressListener<code> method. When
	 * the multiUploadProgress event occurs, that object's appropriate
	 * method is invoked.
	 *
	 * @see MultiUploadProgressEvent
	 */
	private class MultiUploadProgressListener implements ProgressListener {
		
		/** The interval next msg m secs. */
		private long intervalNextMsgMSecs = 10000;	// 10 secs
		
		/** The tot bytes. */
		private long totBytes = 0;
		
		/** The next msg m secs. */
		private long nextMsgMSecs;
		
		/** The started at. */
		private long startedAt;
		
		/**
		 * Instantiates a new multi upload progress listener.
		 */
		public MultiUploadProgressListener() {
			startedAt = System.currentTimeMillis();
			nextMsgMSecs = System.currentTimeMillis() + intervalNextMsgMSecs;
		}
		
		/* (non-Javadoc)
		 * @see com.amazonaws.event.ProgressListener#progressChanged(com.amazonaws.event.ProgressEvent)
		 */
		public void progressChanged(ProgressEvent progressEvent) {
			// TODO: check status code 
			totBytes += progressEvent.getBytesTransferred();
			if( System.currentTimeMillis() > nextMsgMSecs ) {
				long elapsedSecs = (System.currentTimeMillis() - startedAt) / 1000;
				log.info("ProgressEvent: elapsedSecs=" + dfCommas.format(elapsedSecs) 
						+ ", totMBytes=" + dfDec3.format(totBytes/megs) );
				nextMsgMSecs = System.currentTimeMillis() + intervalNextMsgMSecs;
			}
		}
		
		/**
		 * Gets the tot bytes.
		 *
		 * @return the tot bytes
		 */
		public long getTotBytes() {
			return totBytes;
		}
		
	}
	
	/**
	 * Creates the table statement.
	 *
	 * @param dbNameDotTableName the db name dot table name
	 * @return the string
	 * @throws Exception the exception
	 */
	private String createTableStatement( String dbNameDotTableName ) throws Exception {
		final String createTable = 
				"CREATE TABLE " + dbNameDotTableName + " ( " +
					"category1 varchar(512), " +
					"category2 varchar(512), " +
					"category3 varchar(512), " +
					"category4 varchar(512), " +
					"category5 varchar(512), " +
					"category6 varchar(512), " +
					"category7 varchar(512), " +
					"category8 varchar(512), " +
					"category9 varchar(512), " +
					"category10 varchar(512), " +
					"category11 varchar(512), " +
					"category12 varchar(512), " +
					"units varchar(64), " +
					"frequency char(2), " +
					"seasonal_adj char(4), " +
					"last_updated char(10), " +
					"date_series char(10), " +
					"value decimal(38,20), " +
					"country varchar(256), " +
					"city varchar(512), " +
					"county varchar(64), " +
					"state varchar(128), " +
					"region_us varchar(64), " +
					"region_world varchar(64), " +
					"institution varchar(64), " +
					"frb_district varchar(64), " +
					"sex char(1), " +
					"currency varchar(64) ) " +
					"sortkey(category1,category2,category3) ";
		return createTable;
	}
}
