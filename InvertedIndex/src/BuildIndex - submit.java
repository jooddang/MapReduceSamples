package cs492.joogle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;



public class BuildIndex {
	
	private final static String PAGE_START_TAG = "<page>";
	private final static String PAGE_END_TAG = "</page>";
	private final static String TITLE_START_TAG = "<title>";
	private final static String TITLE_END_TAG = "</title>";
	private final static String ID_START_TAG = "<id>";
	private final static String ID_END_TAG = "</id>";
	private final static String TEXT_START_TAG = "<text";
	private final static String TEXT_END_TAG = "</text>";
	private final static String wordSet = "2ne1;apple;batman;computer;doom2;europa;firefox;ghostbusters;hype;intharathit;jellyfish;kaist;lambada;metropolitan;nuclear;olympic;pizza;quasimodo;radiohead;slamdunk;twitter;umbrella;virtual;weezer;xylitol;ynglesdalen;zulu;";
	private static ArrayList<String> selectedWords = null;

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text>{
		
		@Override
		public void map(Text input, Text xmlChunk,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

			InitWords();
			
			String singleXml = xmlChunk.toString();
			
			System.out.println("\n\nsingleXml :[" + singleXml +"]\n");
			
			int titleStart = singleXml.indexOf(TITLE_START_TAG);
			int titleEnd = singleXml.indexOf(TITLE_END_TAG);
			String title = singleXml.substring(titleStart + TITLE_START_TAG.length(), titleEnd);
			
			int idStart = singleXml.indexOf(ID_START_TAG, titleEnd);
			int idEnd = singleXml.indexOf(ID_END_TAG, idStart);
			String id = singleXml.substring(idStart + ID_START_TAG.length(), idEnd);
			
			System.out.println("\n\ntitle:[" + title +"]\n");
			
//			output.collect(new Text(title), new Text(title));
			
			int textStart = singleXml.indexOf(TEXT_START_TAG);
			int textEnd = singleXml.indexOf(TEXT_END_TAG);
			int textStartTagEnds = singleXml.indexOf(">", textStart);
			String text = "";
			
			if (textStartTagEnds + 1 > textEnd)
			{
				System.out.println("textstarttagends:"+ textStartTagEnds + " ; and textend:" + textEnd);
			}
			else
			{
				text = singleXml.substring(textStartTagEnds + 1, textEnd);
			}
			
			System.out.println("\n\ntext :[" + text +"]\n");
			
			StringTokenizer st = new StringTokenizer(text, "=-! []:;(){},.|/*'?&$#@%~+_\t\r\n");
			
			ArrayList<String> avoidDup = new ArrayList<String>();
			String tempToken = "";
			while (st.hasMoreTokens())
			{
				tempToken = st.nextToken();
				if (selectedWords.contains(tempToken.toLowerCase()) == true)
				{
					if (avoidDup.contains(tempToken.toLowerCase()) == false)
					{
						avoidDup.add(tempToken.toLowerCase());
						output.collect(new Text(tempToken), new Text(title));	
					}
				}
			}

			// if title contains white space.....
			StringTokenizer st2 = new StringTokenizer(title, " _");
			while (st2.hasMoreTokens())
			{
				tempToken = st2.nextToken();
				if (selectedWords.contains(tempToken.toLowerCase()) == true)
				{
					if (avoidDup.contains(tempToken.toLowerCase()) == false)
					{
						avoidDup.add(tempToken.toLowerCase());
						output.collect(new Text(tempToken), new Text(title));
					}
				}
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			String temp = "";

			ArrayList<String> avoidDup = new ArrayList<String>();
			while (iter.hasNext()){
				temp = iter.next().toString();
				if (temp.length() > 0)
				{
					if (avoidDup.contains(temp) == false)
					{
						avoidDup.add(temp);
						output.collect(key, new Text(temp));
					}
				}
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
	
		JobConf conf = new JobConf(BuildIndex.class);
		conf.setJobName("BuildIndex");
		
		conf.set("xmlinput.start", PAGE_START_TAG);
		conf.set("xmlinput.end", PAGE_END_TAG);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		 	
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
	}

	private static void InitWords() {
		// TODO Auto-generated method stub
		StringTokenizer st = new StringTokenizer(wordSet, ";");
		int i = 0;
		selectedWords = new ArrayList<String>();
		while (st.hasMoreTokens())
		{
			selectedWords.add(st.nextToken());	
		}
	}
}
