package com.weather;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class WLogger {

	public static void initRootLogger(String logFile) {
		// creates pattern layout
		PatternLayout layout = new PatternLayout();
		String conversionPattern = "%-7p %d [%t] %c %x - %m%n";
		layout.setConversionPattern(conversionPattern);

		// creates console appender
		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.setLayout(layout);
		consoleAppender.activateOptions();

		// creates file appender
		FileAppender fileAppender = new FileAppender();
		fileAppender.setFile(logFile);
		fileAppender.setLayout(layout);
		fileAppender.activateOptions();
		
		Logger.getLogger("org.apache.http").setLevel(Level.ERROR);
		Logger.getLogger("com.amazonaws").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR);
		

		// configures the root logger
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.DEBUG);
		rootLogger.addAppender(consoleAppender);
		rootLogger.addAppender(fileAppender);
	}
}
