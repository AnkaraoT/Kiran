package com.gencsv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;

public class GenCvs {

	private static final Logger LOG = Logger.getLogger(GenCvs.class);

	private static final String EQUITIES_STR = "Equities-";
	private static final String DATE_MMYYYY_STR = "MMyyyy-";
	private static final String CSV_FILE_EXT_STR = ".csv";

	private static final List<String> COLUMNS = Arrays.asList("VOLCKER_DESK","BOOK_CODE", "EXP_DATE", "METHOD", "RISK_CLASS", "ALLOCATION","EXP_LIMIT", "EXP_VALUE", "UTILIZATION", "UNITS");

	public static void main(String[] args) {

		String reportFileName = fileNameCreator();
		//Temporary location for monthly file report
		reportFileName = "D:\\"+reportFileName;
		LOG.info(reportFileName);

		generateCsvFile(reportFileName);
	}

	/**
	 * Auto creating the monthly report file name based on System date
	 * 
	 * @return
	 */
	private static String fileNameCreator() {
		String fileName = EQUITIES_STR;

		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_MMYYYY_STR);
		fileName = fileName + sdf.format(cal.getTime());

		fileName = fileName + CSV_FILE_EXT_STR;

		return fileName;
	}

	/**
	 * Generating the CSV file name for Database data
	 * 
	 * @param filename
	 */
	private static void generateCsvFile(String filename) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String url = "jdbc:oracle:thin:@//hostname:portno/sid";
			Connection conn = DriverManager.getConnection(url, "username",	"password");
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery("SELECT VOLCKER_DESK,BOOK_CODE,EXP_DATE,METHOD,RISK_CLASS,ALLOCATION,EXP_LIMIT,EXP_VALUE,UTILIZATION,UNITS FROM v_monthly_report");
			rset.next();
			FileWriter fname = new FileWriter(filename);
			BufferedWriter bwOutFile = new BufferedWriter(fname);
			StringBuffer sbOutput = new StringBuffer();
			sbOutput.append("VOLCKER_DESK,BOOK_CODE,EXP_DATE,METHOD,RISK_CLASS,ALLOCATION,EXP_LIMIT,EXP_VALUE,UTILIZATION,UNITS");
			bwOutFile.append(sbOutput);
			bwOutFile.append(System.getProperty("line.separator"));
			
			while (rset.next()) {

				bwOutFile.append(rset.getString(1) + "," + rset.getString(2)
						+ "," + rset.getString(3) + "," + rset.getString(4));
				bwOutFile.append(System.getProperty("line.separator"));
				bwOutFile.flush();

			}
			bwOutFile.close();
			stmt.close();
			LOG.info("generateCsvFile : Successfully generated "+filename+" file.");
		}
		catch(SQLException sqe){
			LOG.error("generateCsvFile : Unable to connect database, due to "+sqe.getMessage());
		}
		catch (Exception e) {
			LOG.error("generateCsvFile : Failed to create  "+filename+" file."+e.getMessage());

		}

	}
}
