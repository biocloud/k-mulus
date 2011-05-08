package cbcb.kmulus.util;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CreateChunks{
	/*args[1] = input file. args[2] = output name */	
	public static void main(String args[]){
		String outputFileName = args[2];
		ParseFasta pf = new ParseFasta(args[1]);
		try{
			File outputFile  = new File(outputFileName);
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			String[] record = null;
			while((record = pf.getRecord()) != null){
				bw.wirte(">" + record[0] + "\n" + record[1] + "\n");			
			}
			bw.close();
		}catch (IOException e){
			e.printStackTrace();
		}
	}

}
