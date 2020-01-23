package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
	
	private String fileName;
	BufferedReader br = null;
	
	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		br = new BufferedReader(new FileReader(fileName));
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */
	
	@Override
	public Header getHeader() throws IOException {
		br.mark(1);
		String header = br.readLine();
		String [] headerArr = header.split(",");
		br.reset();
		Header headerObj = new Header(headerArr);
		return headerObj;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */
	
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. 
	 * Note: Return Type of the method will be DataTypeDefinitions
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String headerStr = br.readLine();
		String rowDataStr = br.readLine();
		rowDataStr += " ,";
		br.close();
		Object obj;
		String [] headerDataArr = headerStr.split(",");
		String [] rowDataElements = rowDataStr.split(",");
		String [] rowDataTypes = new String [headerDataArr.length];
		if(rowDataElements!= null) {
			for (int i = 0; i < rowDataElements.length; i++) {
				try {
					obj = Integer.parseInt(rowDataElements[i]);
					if(obj instanceof Integer) {
						rowDataTypes[i] = obj.getClass().getName();
					}
					
				} catch (NumberFormatException nfe) {
					try {
						obj = Double.parseDouble(rowDataElements[i]);
						if(obj instanceof Double) {
							rowDataTypes[i] = obj.getClass().getName();
						}
						
					} catch (Exception e) {
						obj = rowDataElements[i];
						if(obj instanceof String) {
							rowDataTypes[i] = obj.getClass().getName();
						}
						
					}
				}
			}
		}
		DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions(rowDataTypes);
		return dataTypeDefinitions;
	}
}
