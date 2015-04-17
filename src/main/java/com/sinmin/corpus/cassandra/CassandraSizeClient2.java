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

import corpus.sinhala.SinhalaTokenizer;

public class CassandraSizeClient2 {
	private Cluster cluster;
	private Session session;
	PreparedStatement statement;
	long wordcount,bigramcount,trigramcount;
	SinhalaTokenizer tokenizer = new SinhalaTokenizer();

	public void connect(String node,String uname,String pwd) {
		wordcount = 0;bigramcount=0;trigramcount=0;
		cluster = Cluster.builder().addContactPoint(node).withCredentials(uname, pwd).build();
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
		try {
			int wordCount = 0,bigramcount=0,trigramcount=0;
			InputStream in = new FileInputStream(file);
			OMXMLParserWrapper oMXMLParserWrapper = OMXMLBuilderFactory
					.createOMBuilder(in);
			OMElement root = oMXMLParserWrapper.getDocumentElement();

			Iterator<?> postItr = root.getChildElements();
			while (postItr.hasNext()) {
				wordCount=0;bigramcount=0;trigramcount=0;
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

				int yearInt = 0;
				int dayInt = 0;
				int monthInt = 0;

				try {
					yearInt = filterInt(year);
				} catch (Exception e) {

				}
				try {
					dayInt = filterInt(day);
				} catch (Exception e) {
					dayInt = 1;
				}
				try {
					monthInt = filterInt(month);
				} catch (Exception e) {
					monthInt = 1;
				}
				String timestamp = monthInt + "/" + dayInt + "/" + yearInt;
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
				for (int i = 0; i < sentences.length; i++) {
					String[] words = this.splitToWords(sentences[i]);

					for (int j = 0; j < words.length; j++) {
						wordCount++;
						if(j<words.length-1){
							bigramcount++;
						}
						if(j<words.length-2){
							trigramcount++;
						}
					}
					
					
					
				}
				
				statement = session
						.prepare("select * from corpus.bigram_sizes WHERE year=? AND category=?");
				ResultSet results = session.execute(statement.bind(year,category));
				
				Row row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.bigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind(year,category,bigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.bigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + bigramcount, year,category));
				}
				
				statement = session
						.prepare("select * from corpus.bigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind(year,"ALL"));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.bigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind(year,"ALL",bigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.bigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + bigramcount, year,"ALL"));
				}

				statement = session
						.prepare("select * from corpus.bigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind("ALL",category));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.bigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind("ALL",category,bigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.bigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + bigramcount, "ALL",category));
				}
				
				statement = session
						.prepare("select * from corpus.bigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind("ALL","ALL"));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.bigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind("ALL","ALL",bigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.bigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + bigramcount, "ALL","ALL"));
				}
				
				//System.out.println(topic);
				
				statement = session
						.prepare("select * from corpus.trigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind(year,category));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.trigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind(year,category,trigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.trigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + trigramcount, year,category));
				}
				
				statement = session
						.prepare("select * from corpus.trigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind(year,"ALL"));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.trigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind(year,"ALL",trigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.trigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + trigramcount, year,"ALL"));
				}

				statement = session
						.prepare("select * from corpus.trigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind("ALL",category));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.trigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind("ALL",category,trigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.trigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + trigramcount, "ALL",category));
				}
				
				statement = session
						.prepare("select * from corpus.trigram_sizes WHERE year=? AND category=?");
				results = session.execute(statement.bind("ALL","ALL"));
				
				row = results.one();
				if(row == null){
					statement = session
							.prepare("INSERT INTO corpus.trigram_sizes(year,category,size) values (?,?,?)");
					session.execute(statement.bind("ALL","ALL",trigramcount));
				}else{
					statement = session
							.prepare("UPDATE corpus.trigram_sizes SET size = ? WHERE year=? AND category=?");
					session.execute(statement.bind(
							row.getInt("size") + trigramcount, "ALL","ALL"));
				}
				
			}
			
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
	        
	    	LinkedList<String> list = tokenizer.splitWords(sentence);
	        String[] words = new String[list.size()];
	        words = list.toArray(words);
	        return words;
	    }
}
