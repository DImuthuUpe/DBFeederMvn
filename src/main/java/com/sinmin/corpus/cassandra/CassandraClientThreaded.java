package com.sinmin.corpus.cassandra;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMXMLBuilderFactory;
import org.apache.axiom.om.OMXMLParserWrapper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import corpus.sinhala.SinhalaTokenizer;


public class CassandraClientThreaded {

	private Cluster cluster;
	private Session session;
	PreparedStatement statement;
	long wordcount,bigramcount,trigramcount;
	SinhalaTokenizer tokenizer = new SinhalaTokenizer();

	public void connect(String node) {
		wordcount = 0;bigramcount=0;trigramcount=0;
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public void feed(String file) {
		//Runnable wordInsert=new WordInsert();
		
		try {
			InputStream in = new FileInputStream(file);
			OMXMLParserWrapper oMXMLParserWrapper = OMXMLBuilderFactory
					.createOMBuilder(in);
			OMElement root = oMXMLParserWrapper.getDocumentElement();

			Iterator<?> postItr = root.getChildElements();
			while (postItr.hasNext()) {
				OMElement post = (OMElement) postItr.next();

				String content = post.getFirstChildWithName(
						new QName("content")).getText();
				String link = post.getFirstChildWithName(new QName("link"))
						.getText();
				String topic = post.getFirstChildWithName(new QName("topic"))
						.getText();
				String day = post.getFirstChildWithName(new QName("date"))
						.getFirstChildWithName(new QName("day")).getText();
				String month = post.getFirstChildWithName(new QName("date"))
						.getFirstChildWithName(new QName("month")).getText();
				String year = post.getFirstChildWithName(new QName("date"))
						.getFirstChildWithName(new QName("year")).getText();
				String author = post.getFirstChildWithName(new QName("author"))
						.getText();
				String category = post.getFirstChildWithName(
						new QName("category")).getText();

					//System.out.println(topic);
				int yearInt = 0;
				int dayInt = 0;
				int monthInt = 0;

				try {
					yearInt = filterInt(year);
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					dayInt = filterInt(day);
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					monthInt = filterInt(month);
				} catch (Exception e) {
					e.printStackTrace();
				}
				String timestamp = month + "/" + day + "/" + year;
				DateFormat df = new SimpleDateFormat("mm/dd/yyyy");
				Date date = null;
				try {
					date = df.parse(timestamp);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				category = category.charAt(0) + "";
				String[] sentences = this.splitToSentences(content);
				WordInsert wordInsert = null;
			    BigramInsert bigramInsert = null;
			    TrigramInsert trigramInsert = null;
				for (int i = 0; i < sentences.length; i++) {
					
					//System.out.println(sentences[i]);
					String[] words = this.splitToWords(sentences[i]);

					for (int j = 0; j < words.length; j++) {
						//wordInsert.set
						//System.out.println(wordInsert.getState());
						if (words[j].length() > 0) {
							wordInsert=new WordInsert();
					    	wordInsert.connect("127.0.0.1");
							wordInsert.setParams(words[j], wordcount, yearInt,
									category, sentences[i], j, words.length,
									topic, author, link, date);
							wordInsert.start();
							wordcount++;
						}
						
						if(j < words.length-1){
						if (words[j].length() > 0 && words[j + 1].length() > 0) {
							bigramInsert=new BigramInsert();
					    	bigramInsert.connect("127.0.0.1");
							bigramInsert.setParams(words[j], words[j + 1],
									bigramcount, yearInt, category,
									sentences[i], j, words.length, topic,
									author, link, date);
							bigramInsert.start();
							bigramcount++;

						}
						}
						if(j < words.length-2){
						if (words[j].length() > 0 && words[j + 1].length() > 0
								&& words[j + 2].length() > 0) {
							trigramInsert=new TrigramInsert();
					    	trigramInsert.connect("127.0.0.1");
							trigramInsert.setParams(words[j], words[j + 1],
									words[j + 2], trigramcount, yearInt,
									category, sentences[i], j, words.length,
									topic, author, link, date);
							trigramInsert.start();

							trigramcount++;
						}
						}

						try {
							
//								wordInsert.join();
//								bigramInsert.join();
//								trigramInsert.join();
								
//								wordInsert.close();;
//								bigramInsert.close();
//								trigramInsert.close();
								//System.out.println(wordInsert.isAlive());
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
					
					
				}
				
			}System.out.println(wordcount );
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String trim(String s) {
		int len = s.length();
		int st = 0;

		while ((st < len)
				&& (s.charAt(st) == '\u00a0' || s.charAt(st) == ' ' || s
						.charAt(st) == '\u0020')) {
			st++;
		}
		while ((st < len)
				&& (s.charAt(len - 1) == '\u00a0' || s.charAt(st) == ' ' || s
						.charAt(st) == '\u0020')) {
			len--;
		}
		return ((st > 0) || (len < s.length())) ? s.substring(st, len) : s;
	}

	public static void main(String[] args) {
//		CassandraClient cl = new CassandraClient();
//		cl.connect("127.0.0.1");
//		cl.feed("/home/chamila/2014-04-27.xml");
//		System.out.println("dddddddd");
	}
	
	public int filterInt(String number){
        if(number!=null&&number.length()>0){
            final ArrayList<Integer> result = new ArrayList<Integer>();
            // in real life define this as a static member of the class.
            // defining integers -123, 12 etc as matches.
            final Pattern integerPattern = Pattern.compile("(\\-?\\d+)");
            final Matcher matched = integerPattern.matcher(number);
            while (matched.find()) {
                result.add(Integer.valueOf(matched.group()));
            }
            if(result.size()>0){
                return result.get(0);
            }else{
                return -1;
            }
        }else{
            return -1;
        }
	}
	
	  private String[] splitToSentences(String article) {

	        LinkedList<String> list =  tokenizer.splitSentences(article);
	        String[] sentences = new String[list.size()];
	        sentences = list.toArray(sentences);
	        corpus.sinhala.SinhalaVowelLetterFixer vowelLetterFixer = new corpus.sinhala.SinhalaVowelLetterFixer();
	        for(int i=0;i<sentences.length;i++){
	            sentences[i] = vowelLetterFixer.fixText(sentences[i],true);
	        }
	        return sentences;
	    }

	    private String[] splitToWords(String sentence) {
	        /*String raw[]= sentence.split("[\u0020\u002C]");
	        ArrayList<String> w = new ArrayList<>();
	        for (int i=0;i<raw.length;i++){
	            String trimmed = unicodeTrim(raw[i]);
	            if(trimmed!=null&&!trimmed.equals("")){
	                w.add(trimmed);
	            }
	        }
	        String newWords[] = new String[w.size()];
	        newWords = w.toArray(newWords);
	        return newWords;*/
	        LinkedList<String> list = tokenizer.splitWords(sentence);
	        String[] words = new String[list.size()];
	        words = list.toArray(words);
	        return words;
	    }

}
