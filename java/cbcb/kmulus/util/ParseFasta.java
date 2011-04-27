package cbcb.kmulus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.nio.channels.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.io.RandomAccessFile;

/**
 * ParseFasta - class for reading a fasta-like formatted file.
 * 
 * The assumption is that the file contains a set of multi-line records
 * separated by single-line headers starting with a specific separator 
 * (by default ’>’)
 * 
 * - Taken from ParseFasta.pm - Mihai Pop
 * @author cmhill
 *
 */
public class ParseFasta {
	// Head/Record separator, default is ">"
	private String headSep = ">";
	
	// Line seperator used when concatenating the lines in the input
	// forming the body of each record.  Useful for .qual files
	private String lineSep = "";
	
	// Represents the line buffer
	private String buffer = null;
	
	// Buffered reader for the file to be parsed
	private BufferedReader br;
	
	/**
	 * Constructor - Takes a file, using default parameters.
	 * @param file
	 */
	public ParseFasta(File file){
		try{
			// Set the buffered reader to read in the file passed in
			br = new BufferedReader(new FileReader(file));
			
			// Set the string buffer to the first line
			try{
				// Read the first line of the file
				buffer = br.readLine();
				
				// If the file doesnt start with a header, exit
				if(buffer == null || !buffer.startsWith(headSep)){
					System.exit(0);
				}
				
			} catch (IOException e) { e.printStackTrace(); };
			
		} catch(FileNotFoundException e){
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Constructor - Takes a file, header seperator, and line seperator
	 * @param file
	 * @param head
	 * @param lineSep
	 */
	public ParseFasta(File file, String head, String lineSep){
		this.headSep = head;
		this.lineSep = lineSep;
				
		try{
			// Set the buffered reader to read in the file passed in
			br = new BufferedReader(new FileReader(file));
			
			// Set the string buffer to the first line
			try{
				// Read the first line of the file
				buffer = br.readLine();
				
				// If the file doesnt start with a header, exit
				if(buffer == null || !buffer.startsWith(headSep)){
					System.exit(0);
				}
				
			} catch (IOException e) { e.printStackTrace(); };
			
		} catch(FileNotFoundException e){
			e.printStackTrace();
		}
	}	
	
	/**
	 * Returns the head seperator
	 * @return the head seperator
	 */
	public String getHeadSep(){
		return headSep;
	}
	
	/**
	 * Returns the line seperator
	 * @return the line seperator
	 */
	public String getLineSep(){
		return lineSep;
	}
	
	/**
	 * Reads a record and returns the head and data in a String array.
	 * array[0] = head
	 * array[1] = data
	 */
	public String[] getRecord(){
		// Stores head entry
		String head = "";
		
		// Stores the data
		StringBuilder data = new StringBuilder("");
		
		try{			
			// Check to see if the record starts on a header seperator
			if(buffer == null || !buffer.startsWith((headSep))){
				//System.out.println("Record does not start with headsep \"" + headSep + "\" but starts with \""+buffer);
				return null;
			}
			
			// Set the header
			head = buffer.substring(headSep.length());
			
			buffer = br.readLine();
			
			// Set the data by continously looping through the record until a new record
			// is reached or the end of file is reached
			while( buffer != null && !buffer.startsWith(headSep) ){
				//System.out.println("buffer: " + buffer);
				data.append( buffer + this.lineSep ); // Might have to add trim
				buffer = br.readLine();
			}
			
		} catch (IOException e) { e.printStackTrace(); };
		
		// Prepare the record
		String[] results = new 	String[2];
		results[0] = head;
		results[1] = data.toString();
		
		return (results);
	}
	
	/** 
	 * Close the bufferedReader
	 */
	public void closeStream(){
		try{
			this.br.close(); 
		} catch(IOException e) { e.printStackTrace();};
	}
	
	/**
	 * To add:
	 	$parser->seek(posn);
        Resets the parser to a specific location (posn) in the file stream.
	
	 	Reports offset of current record in the input file
	 */
}
