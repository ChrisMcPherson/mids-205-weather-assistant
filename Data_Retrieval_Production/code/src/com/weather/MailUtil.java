package com.weather;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.Logger;

public class MailUtil {
	private final static Logger logger = Logger.getLogger(MailUtil.class);
	
	public static void sendEmail(String subject, String body, String toEmails[], String fileLocation, String fileName) throws Exception {
		try {
			logger.debug("Sending email ...");
			
			String username = PropertiesUtil.getProperty("smtp.userName");
			String password = PropertiesUtil.getProperty("smtp.password");

			final String userNameFinal = username;
			final String passwordFinal = password;

			Properties props = new Properties();
			props.put("mail.smtp.auth", PropertiesUtil.getProperty("smtp.auth"));
			props.put("mail.smtp.starttls.enable", PropertiesUtil.getProperty("smtp.starttls.enable"));
			props.put("mail.smtp.host", PropertiesUtil.getProperty("stmp.host"));
			props.put("mail.smtp.port", PropertiesUtil.getProperty("stmp.port"));

			Session session = Session.getInstance(props,
			new javax.mail.Authenticator() {
				@Override
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(userNameFinal,
							passwordFinal);
				}
			});

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(PropertiesUtil.getProperty("adminEmail")));

			InternetAddress[] addresses = new InternetAddress[toEmails.length];
			for (int i = 0; i < toEmails.length; i++) {
				addresses[i] = new InternetAddress(toEmails[i]);
			}
			message.setRecipients(Message.RecipientType.TO, addresses);
			message.setSubject(subject);
			BodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setText(body);
			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart);
			messageBodyPart = new MimeBodyPart();
			
			if(fileLocation != null && fileName != null) {
				DataSource source = new FileDataSource(fileLocation);
				messageBodyPart.setDataHandler(new DataHandler(source));
				messageBodyPart.setFileName(fileName);
				multipart.addBodyPart(messageBodyPart);
			}
			message.setContent(multipart);
			Transport.send(message);
			logger.debug("Email sent successfully.");
		} catch (Exception ex) {
			logger.error(ex, ex);
			throw ex;
		}
	}
	
	public static void sendApplicationAlert(String subject, String body) {
		try {
			sendEmail(subject, body, PropertiesUtil.getProperty("adminEmail").split(","), null, null);
		} catch (Exception ex) {
			logger.error(ex, ex);
			// can't do much
		}
	}
	
	public static void sendApplicationAlert(String subject, Exception e) {
		try {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			StringBuffer exceptionBuffer = sw.getBuffer();
			StringBuffer finalBuffer = new StringBuffer();
			finalBuffer.append(exceptionBuffer.toString());
			sendEmail(subject, finalBuffer.toString(), PropertiesUtil.getProperty("adminEmail").split(","), null, null);
		} catch(Exception ex) {
			logger.debug(ex, ex);
			//not much we can do - do not throw exception
		}
	}
	
	public static void sendApplicationAlert(Hashtable currentWeatherLog, Hashtable forecastWeatherLog, AWSUtil awsUtil, KafkaClient kafkaClient) {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("<html>");
			sbuf.append("<head>");
			sbuf.append("</head>");
			sbuf.append("<body>");
				sbuf.append("<table>");
	        	sbuf.append("<tr>");
	        	sbuf.append("Current weather processing status by Zone ");
	        	sbuf.append("</tr>");
		        Set<Integer> keys = currentWeatherLog.keySet();
		        for(Integer key: keys){
		        	ZoneWeather zoneWeather = (ZoneWeather)currentWeatherLog.get(key);
		        	sbuf.append("<tr>");
		        	sbuf.append("Zone: "+key);
		        	sbuf.append("</tr>");
		        	sbuf.append("<tr>");
		        	sbuf.append("# of cities selected: "+zoneWeather.getProcessCount());
		        	sbuf.append("</tr>");
		        	sbuf.append("<tr>");
		        	sbuf.append("# of Error: "+zoneWeather.getErrorCount());
		        	sbuf.append("</tr>");
		        }
		        sbuf.append("<tr></tr>");
	        	sbuf.append("<tr>");
	        	sbuf.append("Forecast processing status by Zone ");
	        	sbuf.append("</tr>");
		        keys = forecastWeatherLog.keySet();
		        for(Integer key: keys){
		        	ZoneWeather zoneWeather = (ZoneWeather)forecastWeatherLog.get(key);
		        	sbuf.append("<tr>");
		        	sbuf.append("Zone: "+key);
		        	sbuf.append("</tr>");
		        	sbuf.append("<tr>");
		        	sbuf.append("# of cities selected: "+zoneWeather.getProcessCount());
		        	sbuf.append("</tr>");
		        	sbuf.append("<tr>");
		        	sbuf.append("# of Error: "+zoneWeather.getErrorCount());
		        	sbuf.append("</tr>");
		        }
		        //S3 Status
		        sbuf.append("<tr></tr>");
	        	sbuf.append("<tr>");
	        	sbuf.append("S3 Copying Status: "+(awsUtil.getStatus() == true?"Success":"Failed"));
	        	sbuf.append("</tr>");
	        	if(!awsUtil.getStatus()) {
		        	sbuf.append("<tr>");
		        	sbuf.append("S3 Error Message: "+awsUtil.getErrorMessage());
		        	sbuf.append("</tr>");	
	        	}
	        	
	        	//Kafka Status
	        	sbuf.append("<tr></tr>");
	        	sbuf.append("<tr>");
	        	sbuf.append("Kafka Transfer Status: "+(kafkaClient.getStatus() == true?"Success":"Failed"));
	        	sbuf.append("</tr>");
	        	if(!kafkaClient.getStatus()) {
		        	sbuf.append("<tr>");
		        	sbuf.append("Kafka Error Message: "+kafkaClient.getErrorMessage());
		        	sbuf.append("</tr>");	
	        	}
		        
		        sbuf.append("<tr></tr>");
		        //dump log
		        sbuf.append("Current weather error log:");
		        keys = currentWeatherLog.keySet();
		        for(Integer key: keys){
		        	ZoneWeather zoneWeather = (ZoneWeather)currentWeatherLog.get(key);
		        	List<String> errorLog = zoneWeather.getErrorLog();
		        	sbuf.append("Error log for Zone: "+key);
		        	for(String log: errorLog) {
			        	sbuf.append("<tr>");
			        	sbuf.append(log);
			        	sbuf.append("</tr>");
		        	}
		        }
		        sbuf.append("<tr></tr>");
		        sbuf.append("Forecast weather error log:");
		        keys = forecastWeatherLog.keySet();
		        for(Integer key: keys){
		        	ZoneWeather zoneWeather = (ZoneWeather)forecastWeatherLog.get(key);
		        	List<String> errorLog = zoneWeather.getErrorLog();
		        	sbuf.append("Error log for Zone: "+key);
		        	for(String log: errorLog) {
			        	sbuf.append("<tr>");
			        	sbuf.append(log);
			        	sbuf.append("</tr>");
		        	}
		        }
		        

		        sbuf.append("</table>");
			sbuf.append("</body>");
		sbuf.append("</html>");
		logger.debug(sbuf.toString());
		sendApplicationAlert("W205 Project - Weather Processing Status", sbuf.toString());
	}	
	
	
	public static void main(String args[]) {
		System.out.println(PropertiesUtil.getProperty("adminEmail"));
		Exception ex = new Exception("Test");
		sendApplicationAlert("Test", ex);
		System.out.println("Done");
	}
}