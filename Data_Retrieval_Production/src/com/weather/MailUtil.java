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
			message.setFrom(new InternetAddress(username));

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

	public static void sendApplicationAlert(Hashtable currentWeatherLog, Hashtable forecastWeatherLog, AWSUtil awsUtil, KafkaClient kafkaClient, int processingHour) {
		StringBuffer sbuf = new StringBuffer();
		//Kafka Status
		sbuf.append("Kafka Upload Status: "+(kafkaClient.getStatus() == true?"Success":"Failed"));
		sbuf.append("\r\n");
		if(!kafkaClient.getStatus()) {
			sbuf.append("\r\n");
			sbuf.append("Kafka Error Message: "+kafkaClient.getErrorMessage());
			sbuf.append("\r\n");
		}

		sbuf.append("\r\n");
		sbuf.append("\r\n");
		//S3 Status
		sbuf.append("S3 Upload Status: "+(awsUtil.getStatus() == true?"Success":"Failed"));
		sbuf.append("\r\n");
		if(!awsUtil.getStatus()) {
			sbuf.append("\r\n");
			sbuf.append("S3 Error Message: "+awsUtil.getErrorMessage());
			sbuf.append("\r\n");
		}

		sbuf.append("\r\n");
		sbuf.append("\r\n");
		sbuf.append("Current weather processing status by Zone: ");
		sbuf.append("\r\n");
		Set<Integer> keys = currentWeatherLog.keySet();
		for(Integer key: keys){
			ZoneWeather zoneWeather = (ZoneWeather)currentWeatherLog.get(key);
			sbuf.append("\r\n");
			sbuf.append("Zone: "+key);
			sbuf.append("\r\n");
			sbuf.append("# of cities selected: "+zoneWeather.getProcessCount());
			sbuf.append("\r\n");
			sbuf.append("# of Cities processed successfully: "+(zoneWeather.getProcessCount() - zoneWeather.getErrorCount()));
			sbuf.append("\r\n");
			sbuf.append("# of Error responses: "+zoneWeather.getErrorCount());
			sbuf.append("\r\n");
		}
		sbuf.append("\r\n");
		sbuf.append("\r\n");
		sbuf.append("Forecast processing status by Zone: ");
		sbuf.append("\r\n");
		keys = forecastWeatherLog.keySet();
		for(Integer key: keys){
			ZoneWeather zoneWeather = (ZoneWeather)forecastWeatherLog.get(key);
			sbuf.append("Zone: "+key);
			sbuf.append("\r\n");
			sbuf.append("# of cities selected: "+zoneWeather.getProcessCount());
			sbuf.append("\r\n");
			sbuf.append("# of cities processed successfully: "+(zoneWeather.getProcessCount() - zoneWeather.getErrorCount()));
			sbuf.append("\r\n");
			sbuf.append("# of error responses: "+zoneWeather.getErrorCount());
			sbuf.append("\r\n");
		}

		sbuf.append("\r\n");
		//dump log
		sbuf.append("Current weather error log:");
		sbuf.append("\r\n");
		keys = currentWeatherLog.keySet();
		for(Integer key: keys){
			ZoneWeather zoneWeather = (ZoneWeather)currentWeatherLog.get(key);
			List<String> errorLog = zoneWeather.getErrorLog();
			sbuf.append("\r\n");
			sbuf.append("Error log for Zone: "+key);
			sbuf.append("\r\n");
			for(String log: errorLog) {
				sbuf.append("\r\n");
				sbuf.append(log);
				sbuf.append("\r\n");
			}

		}
		sbuf.append("\r\n");
		sbuf.append("Forecast weather error log:");
		sbuf.append("\r\n");
		keys = forecastWeatherLog.keySet();
		for(Integer key: keys){
			ZoneWeather zoneWeather = (ZoneWeather)forecastWeatherLog.get(key);
			if(zoneWeather.getErrorCount() > 0) {
				List<String> errorLog = zoneWeather.getErrorLog();
				sbuf.append("\r\n");
				sbuf.append("Error log for Zone: "+key);
				sbuf.append("\r\n");
				for(String log: errorLog) {
					sbuf.append("\r\n");
					sbuf.append(log);
					sbuf.append("\r\n");
				}
			}
		}
		logger.debug(sbuf.toString());
		sendApplicationAlert("W205 Project - Weather Processing Hour: "+processingHour, sbuf.toString());
	}	


	public static void main(String args[]) {
		System.out.println(PropertiesUtil.getProperty("adminEmail"));
		Exception ex = new Exception("Test");
		sendApplicationAlert("Test", ex);
		System.out.println("Done");
	}
}