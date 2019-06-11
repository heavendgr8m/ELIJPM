package client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.io.StringReader;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
//import java.util.Base64;
import org.apache.commons.codec.binary.Base64;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//import javax.mail.PasswordAuthentication;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import com.finiq.db_functions.DB_Functions;
import com.finiq.exception.FinIQDBConnectionException;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import util.ConfigReader;
import util.DBUtil;
import java.net.PasswordAuthentication;

public class DownloadTermsheets implements Runnable {
	private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(DownloadTermsheets.class.getName());
	private static Connection conn = null;
	private static final int BUFFER_SIZE = 4096;
	private static final Properties config = ConfigReader.getConfigFile();
	private String xmlMsg = "";
	private String TS_Type = "";

	public DownloadTermsheets(String xmlMsg, String TS_Type) throws Exception {
		this.xmlMsg = xmlMsg;
		this.TS_Type = TS_Type;
	}

	@Override
	public void run() {
		LOG.info(" Thread Started " + Thread.currentThread().getName() + " with id : " + Thread.currentThread().getId());
		int waitForConnection = Integer.parseInt(config.getProperty("Conn_Wait_Interval"));

		try {
			for (int i = 0; i < 10; i++) {
				try {
					if (conn == null || !conn.isValid(0)) {
						conn = DBUtil.getDBConnection();
					}
					break;
				} catch (SQLException e) {
					Thread.sleep(waitForConnection);
					LOG.warn("Thread : "+ Thread.currentThread().getId() + " Attempt " + i + ": Failed to connect to database. Retrying...", e);
				}
			}

			if (conn.isValid(0)) {
				if (TS_Type.equals("Indicative")) {
					parseQuoteResponseXML();
				} else {
					parseOrderResponseXML();
				}
			} else {
				LOG.error("Thread : "+ Thread.currentThread().getId() + ". Database connection lost. ");
				throw new FinIQDBConnectionException("Thread : "+ Thread.currentThread().getId() + ". FinIQDBConnectionException: Database connection lost.");
			}

		} catch (Exception e) {
			LOG.error("Thread : "+ Thread.currentThread().getId() +". Error in DownloadTermsheets thread DB connection ", e);
		}
	}

	private void parseQuoteResponseXML() throws SQLException {

		String URL = "";
		String Filename = "";
		String Language = "";
		String seriesID = "";
		String strJPMQuoteID = "";

		try {

			JAXBContext jc = JAXBContext.newInstance(com.finiq.Confirm_Mandate_xsd.ObjectFactory.class);
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			StringReader reader = new StringReader(xmlMsg);
			com.finiq.Confirm_Mandate_xsd.Payload msg = (com.finiq.Confirm_Mandate_xsd.Payload) unmarshaller
					.unmarshal(reader);

			String attachFiles = "";
			seriesID = msg.getSeriesID().toString();
			strJPMQuoteID = msg.getJPMQuoteID().toString();

			LOG.info("Thread : "+ Thread.currentThread().getId() +". Series ID : " + seriesID);

			for (int i = 0; i < msg.getTermsheet().getDocumentation().get(0).getDocument().size(); i++) {
				// seriesID =
				// msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getSeriesID();
				URL = msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getURL();
				Filename = msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getFileName();
				Language = msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getLanguage();
				
				if (config.getProperty("TermsheetDownloadProtocol","HTTP").equalsIgnoreCase("HTTP")) {
					
				String Filepath = invokeURL(URL, Filename, seriesID);
				
				if (attachFiles == "") {
					attachFiles += Filepath;
				} else {
					attachFiles += ";" + Filepath;
				}
				}
				else if (config.getProperty("TermsheetDownloadProtocol","HTTP").equalsIgnoreCase("SFTP")) {

					String Filepath = invokeSFTP( Filename, TS_Type);
					
					if (attachFiles == "") {
						attachFiles += Filepath;
					} else {
						attachFiles += ";" + Filepath;
					}
					}
			}

			try {
				if(attachFiles != "")
					DB_Functions.updateTermsheeturl("Indicative", attachFiles, "", seriesID, strJPMQuoteID, "", conn);
				LOG.info("Thread : "+ Thread.currentThread().getId() +". Indicative URL and Schedule saved successfully for Quote : " + msg.getJPMQuoteID());
			} catch (Exception e) {
				LOG.error("Thread : "+ Thread.currentThread().getId() +". Error occured while saving Termsheet URL and Schedule Data in DB ", e);
			}
			// finally{
			// Thread.currentThread().destroy();
			// }

		} catch (JAXBException e) {
			LOG.error("Error while unmarshalling xml : ", e);
		} catch (Exception ex) {
			LOG.error("Error occurred while downloading termsheet : ", ex);
		}
	}

	private void parseOrderResponseXML() throws SQLException {
		String URL = "";
		String Filename = "";
		String Language = "";
		String seriesID = "";
		String ISIN = "";
		try {

			JAXBContext jc = JAXBContext.newInstance(com.finiq.Confirm_Mandate_xsd.ObjectFactory.class);
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			StringReader reader = new StringReader(xmlMsg);
			com.finiq.Confirm_Mandate_xsd.Payload msg = (com.finiq.Confirm_Mandate_xsd.Payload) unmarshaller
					.unmarshal(reader);

			String attachFiles = "";

			try {
				ISIN = msg.getISIN();
			} catch (Exception ex) {
				ISIN = "";
				LOG.info("ISIN not present in order restate.");
			}

			seriesID = msg.getJPMQuoteID();

			for (int i = 0; i < msg.getTermsheet().getDocumentation().get(0).getDocument().size(); i++) {
				URL = msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getURL();
				Filename = msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getFileName();
				Language = msg.getTermsheet().getDocumentation().get(0).getDocument().get(i).getLanguage();

				String Filepath = invokeURL(URL, Filename, seriesID);

				if (attachFiles == "") {
					attachFiles += Filepath;
				} else {
					attachFiles += ";" + Filepath;
				}
			}

			try {
				DB_Functions.updateTermsheeturl("Final", attachFiles, "", seriesID, ISIN, "JPM", conn);
				LOG.info("Thread : "+ Thread.currentThread().getId() + ". Final Termsheet URL saved successfully for Quote : " + seriesID);
			} catch (Exception e) {
				LOG.error("Thread : "+ Thread.currentThread().getId() + ". Error occured while savind Termsheet URL in DB ", e);
			}
			// finally{
			// Thread.currentThread().destroy();
			// }
		} catch (JAXBException e) {
			LOG.error("Thread : "+ Thread.currentThread().getId() + ". Error while unmarshalling xml : ", e);
		} catch (Exception ex) {
			LOG.error("Thread : "+ Thread.currentThread().getId() + ". Error occurred while downloading termsheet : ", ex);
		}
	}

	public String invokeURL(String fileURLs, String fileName, String seriesID)
			throws NumberFormatException, InterruptedException {
		
		String saveFilePath = "";
		String user = config.getProperty("LinkUser");
		String Password = config.getProperty("LinkPassword");
		byte[] authEncBytes = null;
		int responseCode = 0;
		String authString = "";
		String file_type = "";
		
		try {
			
			HttpURLConnection conn=null;
			fileURLs = fileURLs + fileName + "/";

			CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));

			String saveDir = "";
			if (TS_Type.equals("Indicative")) {
				saveDir = config.getProperty("saveDirectory_Indicative");
			} else {
				saveDir = config.getProperty("saveDirectory_Final");
			}

			if (fileName.toUpperCase().endsWith("DOCX")) {
				file_type = ".DOCX";
			} else if (fileName.toUpperCase().endsWith("DOC")) {
				file_type = ".DOC";

			} else if (fileName.toUpperCase().endsWith("PDF")) {
				file_type = ".PDF";
			}

			if (!fileName.toUpperCase().contains("TERMSHEET")) {
				String[] arrFileName = fileName.split("\\.");
				fileName = arrFileName[0] + "_" + seriesID.trim();
				fileName = fileName + file_type;
			}
			LOG.info("Thread : "+ Thread.currentThread().getId() + ". fileName = " + fileName);

			authString = user + ":" + Password;
			authEncBytes = Base64.encodeBase64(authString.getBytes());
			String authStringEnc = new String(authEncBytes);

			/*Authenticator.setDefault(new Authenticator() {

				@Override
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(user, Password.toCharArray());
				}
			});*/

			URL url = new URL(fileURLs);

			// <Changed By : Pooja S/Dhanshri S || for termsheet download retry ||
			// 24-Jan-2019>
			/*
			 * conn = (HttpURLConnection) url.openConnection();
			 * conn.setInstanceFollowRedirects(false);
			 * HttpURLConnection.setFollowRedirects(false);
			 * 
			 * conn.setRequestProperty("Authorization", "Basic " + authStringEnc);
			 * 
			 * responseCode = conn.getResponseCode();
			 * 
			 * url = new URL(fileURLs); conn = (HttpURLConnection) url.openConnection();
			 */
			// conn.connect();

			for (int i = 0; i < Integer.parseInt(config.getProperty("TermsheetDownloadAttempts", "3")); i++) {
				
				boolean exceptionFlag = false;
				
				LOG.info("Thread : "+ Thread.currentThread().getId() +". First URL is : " + url.toString());
				conn = (HttpURLConnection) url.openConnection();
				conn.setInstanceFollowRedirects(false);
				HttpURLConnection.setFollowRedirects(false);
				conn.setRequestProperty("Authorization", "Basic " + authStringEnc);
				
				/*LOG.info("All Request Headers for URL: " + url.toString() + "\n");
				for (String header : conn.getRequestProperties().keySet()) {
				   if (header != null) {
				     for (String value : conn.getRequestProperties().get(header)) {
				    	 LOG.info(header + " : " + value);
				      }
				   }
				}*/
				LOG.info("Content-type: " + conn.getContentType());
				LOG.info("Content-encoding: " + conn.getContentEncoding());
				LOG.info("Content-length: " + conn.getContentLength());
				LOG.info("Date: " + new Date(conn.getDate()));
				LOG.info("Last modified: " + new Date(conn.getLastModified()));
				LOG.info("Expiration date: " + new Date(conn.getExpiration()));			
				LOG.info("Connection timeout : " + conn.getConnectTimeout());			
				LOG.info("Read timeout : " + conn.getReadTimeout());			
				LOG.info("Request method : " + conn.getRequestMethod());
				LOG.info("Authorization : " + conn.getHeaderField("Authorization"));
				
				LOG.info("First Connection formed is : " + conn.toString());
				responseCode = conn.getResponseCode();
				LOG.info("Thread : "+ Thread.currentThread().getId() +". 1st response code is " + responseCode + " for tranche "+seriesID);
				
				///<Amol S: 2-May-2019: added delay to stable HTTP conection >
				Thread.sleep(Integer.parseInt(config.getProperty("UrlHitDelayInterval", "500")));
				//TOCheck - Add delay
				
				url = new URL(fileURLs);
				LOG.info("Thread : "+ Thread.currentThread().getId() +". Second URL is : " + url.toString());
				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestProperty("Authorization", "Basic " + authStringEnc);
				
				LOG.info("Content-type: " + conn.getContentType());
				LOG.info("Content-encoding: " + conn.getContentEncoding());
				LOG.info("Date: " + new Date(conn.getDate()));
				LOG.info("Last modified: " + new Date(conn.getLastModified()));
				LOG.info("Expiration date: " + new Date(conn.getExpiration()));
				LOG.info("Content-length: " + conn.getContentLength());
				LOG.info("Connection timeout : " + conn.getConnectTimeout());			
				LOG.info("Read timeout : " + conn.getReadTimeout());			
				LOG.info("Request method : " + conn.getRequestMethod());
				LOG.info("Authorization : " + conn.getHeaderField("Authorization"));
				
				LOG.info("Second Connection formed is : " + conn.toString());
				responseCode = conn.getResponseCode();
				LOG.info("Thread : "+ Thread.currentThread().getId() +". 2nd response code is " + responseCode + " for tranche "+seriesID);

				//LOG.info("responseCode for Tranche: " + seriesID + ":" + responseCode);

				//<Pooja S (09-May-2019) : Added logs to print response headers for termsheet download issue investigation>
				
				
				Map<String, List<String>> map = conn.getHeaderFields();				
				LOG.info("Thread : "+ Thread.currentThread().getId() +". All Response Headers for URL: " + url.toString() + "\n");
				for (Map.Entry<String, List<String>> entry : map.entrySet()) {
					LOG.info(entry.getKey() + " : " + entry.getValue());
				}
				//<Pooja S (09-May-2019) : Added logs to print response headers for termsheet download issue investigation/>
				
				if (responseCode == HttpURLConnection.HTTP_OK) {
					try {
						LOG.info("Starting downloading for tranche No.:"+ seriesID + " and fileName :"+ fileName);
						String EncryptedFileName = encrypt(fileName) + "=" + file_type;
						saveFilePath = saveDir + File.separator + EncryptedFileName;
						////<Amol S/ Pooja S: 2-May-2019 Try-Finally added to close resources hold by below code >
						BufferedInputStream inputStream =null;
						FileOutputStream outputStream = null;
						try {
							inputStream = new BufferedInputStream(conn.getInputStream());
							outputStream = new FileOutputStream(saveFilePath);
							int bytesRead = -1;
							byte[] buffer = new byte[BUFFER_SIZE];
							while ((bytesRead = inputStream.read(buffer)) != -1) {
								outputStream.write(buffer, 0, bytesRead);
							}
//							outputStream.close();
//							inputStream.close();
						} finally {
							// TODO: handle finally clause
							if(outputStream!=null)
								outputStream.close();
							if(inputStream!=null)
								inputStream.close();
						}
						LOG.info("Thread : "+ Thread.currentThread().getId() + ". Files are successfully saved at" + saveFilePath);
						///<Amol S: 2-May-2019: added delay to stable HTTP conection >
						Thread.sleep(Integer.parseInt(config.getProperty("UrlHitDelayInterval", "500")));
						break;
					} catch (Exception e) {
						LOG.error("Thread : "+ Thread.currentThread().getId() + ". Error occurred while downloading termsheet " + fileName, e);
						exceptionFlag = true;
					}
				} else {
					/*LOG.info("No file to download. Server replied HTTP code: " + responseCode);
					Thread.sleep(Integer.parseInt(config.getProperty("TermshhetDownloadInterval", "10000")));
					LOG.info("Retrying attempt : " + (i + 1));*/
					exceptionFlag = true;
				}
				
				
				if (exceptionFlag) {
					LOG.info("No file to download. Server replied HTTP code: " + responseCode);
					Thread.sleep(Integer.parseInt(config.getProperty("TermshhetDownloadInterval", "10000")));
					LOG.info("Retrying attempt : " + (i + 1));
					saveFilePath = "";
				}
				/*
				 * //to clear cookies CookieManager cookieManager = (CookieManager)
				 * CookieHandler .getDefault(); cookieManager.getCookieStore().removeAll();
				 */
				if(conn!=null)
					conn.disconnect();
			}

			// <//Changed By : Pooja S/Dhanshri S || for termsheet download retry ||
			// 24-Jan-2019>
		} catch (MalformedURLException e) {
			LOG.error("Thread : "+ Thread.currentThread().getId() +". Error occurred while downloading termsheets.", e);
		} catch (IOException e) {
			LOG.error("Thread : "+ Thread.currentThread().getId() +". Error occurred while downloading termsheets.", e);
		}
		return saveFilePath;
	}

	// Added by Gopika W on 13-April-2018 || to make encrypted http termsheet
	// downloadable link || Start
	public static String encrypt(String plainText) {
		String encryptedText = "";
		try {

			// byte[] encodedBytes = Base64.getEncoder().encode(plainText.getBytes());
			byte[] encodedBytes = Base64.encodeBase64(plainText.getBytes());
			encryptedText = new String(encodedBytes);

		} catch (Exception E) {
			LOG.error("Thread : "+ Thread.currentThread().getId() +". Encrypt Exception : ", E);
		}
		return encryptedText;
	}
	
	// Added by Gopika W on 13-April-2018 || to make encrypted http termsheet
	// downloadable link || End
	
	//Added for SFTP Termsheet Download by Dhanashri S. on 27-May-2019 asked by Goraksha J./ Amol S.
	
	public static String invokeSFTP (String Filename, String Termsheet_Type) {
		String serverAddress = config.getProperty("SFTP_Host");
		String saveFilePath = "";
		String user = config.getProperty("SFTP_User");
		String Password = config.getProperty("SFTP_Password");
		String file_type = "";
		String localDir = "";
		String remoteDir = "";
		
		try {
			
			
			if (Termsheet_Type.equals("Indicative")) {
				localDir = config.getProperty("SFTP_localDir_Indicative");
				remoteDir = config.getProperty("SFTP_remoteDir_Indicative");
			} else {
				localDir = config.getProperty("SFTP_localDir_Final");
				remoteDir = config.getProperty("SFTP_remoteDir_Final");
			}

			if (Filename.toUpperCase().endsWith("DOCX")) {
				file_type = ".DOCX";
			} else if (Filename.toUpperCase().endsWith("DOC")) {
				file_type = ".DOC";

			} else if (Filename.toUpperCase().endsWith("PDF")) {
				file_type = ".PDF";
			}

			if (!Filename.toUpperCase().contains("TERMSHEET")) {
				String[] arrFileName = Filename.split("\\.");
				Filename = arrFileName[0] + "_" + Filename.trim();
				Filename = Filename + file_type;
			}
			LOG.info("Thread : "+ Thread.currentThread().getId() + ". fileName = " + Filename);

			remoteDir = remoteDir + "/" + Filename;
			
			String EncryptedFileName = encrypt(Filename) + "=" + file_type;
		    LOG.info("Saving "+ Filename + "as " + EncryptedFileName);
			saveFilePath = localDir + File.separator + EncryptedFileName;
			
			JSch jsch = new JSch();
			
			java.util.Properties config = new java.util.Properties(); 
			config.put("StrictHostKeyChecking", ConfigReader.getConfigFile().getProperty("SFTP_StrictHostKeyChecking","no").trim());
			
			Session session = jsch.getSession( user,serverAddress);  
			{
			session.setPassword( Password );
			}
			session.setConfig(config);
			session.connect();

			Channel channel = session.openChannel( "sftp" );
			channel.connect();

			ChannelSftp sftpChannel = (ChannelSftp) channel;

			sftpChannel.get(remoteDir,localDir);

			sftpChannel.exit();
			session.disconnect();
		}catch(Exception e) {
			LOG.error("Error in downloading termsheets by SFTP ", e);
			
		}

		return saveFilePath;
		
	
	}

}
