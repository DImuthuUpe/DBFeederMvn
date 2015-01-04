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


public class CassandraClient {

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

				}
				try {
					monthInt = filterInt(month);
				} catch (Exception e) {

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
				for (int i = 0; i < sentences.length; i++) {
					String[] words = this.splitToWords(sentences[i]);

					for (int j = 0; j < words.length; j++) {
						if (words[j].length() > 0) {
							statement = session
									.prepare("select * from corpus.word_time_category_frequency WHERE word=? AND year=? AND category=?");
							ResultSet results = session.execute(statement.bind(
									words[j], yearInt, category));
							// System.out.println("3 right");
							Row row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_time_category_frequency(id, word, year, category, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										yearInt, category, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.word_time_category_ordered_frequency(id, word, year, category, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										yearInt, category, 1));
								// System.out.println("4b right");
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_time_category_frequency SET frequency = ? WHERE word=? AND year=? AND category=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										yearInt, category));
								
								statement = session
										.prepare("DELETE FROM corpus.word_time_category_ordered_frequency WHERE word=? AND year=? AND category=? AND frequency = ?");
								session.execute(statement.bind( words[j],
										yearInt, category, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.word_time_category_ordered_frequency(id, word, year, category, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										yearInt, category, row.getInt("frequency") + 1));
								// System.out.println("4a right");
							}
							
							// //////////////////////////////////////////////////////
							
							statement = session
									.prepare("select * from corpus.word_frequency WHERE word=?");
							results = session.execute(statement.bind(words[j]));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_frequency(id, word,frequency) values (?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										1));
								
								// System.out.println("4b right");
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_frequency SET frequency = ? WHERE word=? ");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j]));
								
								// System.out.println("4a right");
							}

							// ///////////////////////////////////////////////////////////////

							statement = session
									.prepare("select * from corpus.word_time_frequency WHERE word=? AND year=?");
							results = session.execute(statement.bind(words[j],
									yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_time_frequency(id, word,year,frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										yearInt, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.word_time_ordered_frequency(id, word, year, frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										yearInt,  1));
								
								// System.out.println("4b right");
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_time_frequency SET frequency = ? WHERE word=? and year=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										yearInt));
								
								statement = session
										.prepare("DELETE FROM corpus.word_time_ordered_frequency WHERE word=? AND year=?  AND frequency = ?");
								session.execute(statement.bind( words[j],
										yearInt,  row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.word_time_ordered_frequency(id, word, year, frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										yearInt, row.getInt("frequency") + 1));
							}

							// ///////////////////////////////////////////

							statement = session
									.prepare("select * from corpus.word_category_frequency WHERE word=? AND category=?");
							results = session.execute(statement.bind(words[j],
									category));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_category_frequency(id, word,category,frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										category, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.word_category_ordered_frequency(id, word, category, frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										category, 1));
								// System.out.println("4b right");
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_category_frequency SET frequency = ? WHERE word=? and category=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										category));
								
								statement = session
										.prepare("DELETE FROM corpus.word_category_ordered_frequency WHERE word=?  AND category=? AND frequency = ?");
								session.execute(statement.bind( words[j],
										category, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.word_category_ordered_frequency(id, word, category, frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],
										category, row.getInt("frequency") + 1));
							}
							
							statement= session.prepare(
									"INSERT INTO corpus.word_usage (id, word, sentence, postname, date, url,author) values (?,?,?,?,?,?,?)");
						    session.execute(statement.bind(wordcount,words[j],sentences[i],topic,date,link,author));
						
						    statement= session.prepare(
									"INSERT INTO corpus.word_year_usage (id, word, sentence, postname, year, date, url, author) values (?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(wordcount,words[j],sentences[i],topic,yearInt,date,link,author));
						    
						    statement= session.prepare(
									"INSERT INTO corpus.word_category_usage (id, word, sentence, postname, category, date, url, author) values (?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(wordcount,words[j],sentences[i],topic,category,date,link,author));
						    
						    statement= session.prepare(
									"INSERT INTO corpus.word_year_category_usage (id, word, sentence, postname, year, category, date, url, author) values (?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(wordcount,words[j],sentences[i],topic,yearInt, category,date,link,author));
							
						    //////////////////////////////////
						    
						    statement = session
									.prepare("select * from corpus.word_pos_id WHERE word=? AND position=?");
							results = session.execute(statement.bind(words[j],j));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_pos_frequency(id, word,position,frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_id(id, word,position,frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_pos_id SET frequency = ? WHERE word=? AND position=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],j));
								
								statement = session
										.prepare("DELETE FROM corpus.word_pos_frequency WHERE word=? AND position=? AND frequency = ?");
								session.execute(statement.bind( words[j],j, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_frequency(id, word, position, frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j, row.getInt("frequency") + 1));

							}
							
							statement = session
									.prepare("select * from corpus.word_inv_pos_id WHERE word=? AND inv_position=?");
							results = session.execute(statement.bind(words[j],words.length-1-j));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_frequency(id, word,inv_position,frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_id(id, word,inv_position,frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_inv_pos_id SET frequency = ? WHERE word=? AND inv_position=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],words.length-1-j));
								
								statement = session
										.prepare("DELETE FROM corpus.word_inv_pos_frequency WHERE word=? AND inv_position=? AND frequency = ?");
								session.execute(statement.bind( words[j],words.length-1-j, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_frequency(id, word, inv_position, frequency) values (?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j, row.getInt("frequency") + 1));

							}
						    
						    //////////////////////////////////////////////////////////////////
						    
							statement = session
									.prepare("select * from corpus.word_pos_year_id WHERE word=? AND position=? AND year=?");
							results = session.execute(statement.bind(words[j],j,yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_pos_year_frequency(id, word,position,frequency,year) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1,yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_year_id(id, word,position,frequency,year) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1,yearInt));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_pos_year_id SET frequency = ? WHERE word=? AND position=? AND year=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],j,yearInt));
								
								statement = session
										.prepare("DELETE FROM corpus.word_pos_year_frequency WHERE word=? AND position=? AND frequency = ? AND year=?");
								session.execute(statement.bind( words[j],j, row.getInt("frequency"),yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_year_frequency(id, word, position, frequency,year) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j, row.getInt("frequency") + 1,yearInt));

							}
							
							statement = session
									.prepare("select * from corpus.word_inv_pos_year_id WHERE word=? AND inv_position=? AND year=?");
							results = session.execute(statement.bind(words[j],words.length-1-j,yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_year_frequency(id, word,inv_position,frequency,year) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1,yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_year_id(id, word,inv_position,frequency,year) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1,yearInt));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_inv_pos_year_id SET frequency = ? WHERE word=? AND inv_position=? AND year=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],words.length-1-j,yearInt));
								
								statement = session
										.prepare("DELETE FROM corpus.word_inv_pos_year_frequency WHERE word=? AND inv_position=? AND frequency = ? AND year=?");
								session.execute(statement.bind( words[j],words.length-1-j, row.getInt("frequency"),yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_year_frequency(id, word, inv_position, frequency,year) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j, row.getInt("frequency") + 1,yearInt));

							}
							
							
							/////////////////////////////////////////////////////////////////////////
							
							statement = session
									.prepare("select * from corpus.word_pos_category_id WHERE word=? AND position=? AND category=?");
							results = session.execute(statement.bind(words[j],j,category));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_pos_category_frequency(id, word,position,frequency,category) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1,category));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_category_id(id, word,position,frequency,category) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1,category));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_pos_category_id SET frequency = ? WHERE word=? AND position=? AND category=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],j,category));
								
								statement = session
										.prepare("DELETE FROM corpus.word_pos_category_frequency WHERE word=? AND position=? AND frequency = ? AND category=?");
								session.execute(statement.bind( words[j],j, row.getInt("frequency"),category));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_category_frequency(id, word, position, frequency,category) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j, row.getInt("frequency") + 1,category));

							}
							
							statement = session
									.prepare("select * from corpus.word_inv_pos_category_id WHERE word=? AND inv_position=? AND category=?");
							results = session.execute(statement.bind(words[j],words.length-1-j,category));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_category_frequency(id, word,inv_position,frequency,category) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1,category));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_category_id(id, word,inv_position,frequency,category) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1,category));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_inv_pos_category_id SET frequency = ? WHERE word=? AND inv_position=? AND category=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],words.length-1-j,category));
								
								statement = session
										.prepare("DELETE FROM corpus.word_inv_pos_category_frequency WHERE word=? AND inv_position=? AND frequency = ? AND category=?");
								session.execute(statement.bind( words[j],words.length-1-j, row.getInt("frequency"),category));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_category_frequency(id, word, inv_position, frequency,category) values (?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j, row.getInt("frequency") + 1,category));

							}
							
							
							//////////////////////////////////////////////////////////
							
							statement = session
									.prepare("select * from corpus.word_pos_year_category_id WHERE word=? AND position=? AND category=? AND year=?");
							results = session.execute(statement.bind(words[j],j,category,yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_pos_year_category_frequency(id, word,position,frequency,category,year) values (?,?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1,category,yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_year_category_id(id, word,position,frequency,category,year) values (?,?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j,1,category,yearInt));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_pos_year_category_id SET frequency = ? WHERE word=? AND position=? AND category=? AND year=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],j,category,yearInt));
								
								statement = session
										.prepare("DELETE FROM corpus.word_pos_year_category_frequency WHERE word=? AND position=? AND frequency = ? AND category=? AND year=?");
								session.execute(statement.bind( words[j],j, row.getInt("frequency"),category,yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_pos_year_category_frequency(id, word, position, frequency,category,year) values (?,?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],j, row.getInt("frequency") + 1,category,yearInt));

							}
							
							statement = session
									.prepare("select * from corpus.word_inv_pos_year_category_id WHERE word=? AND inv_position=? AND category=? AND year=?");
							results = session.execute(statement.bind(words[j],words.length-1-j,category,yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_year_category_frequency(id, word,inv_position,frequency,category,year) values (?,?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1,category,yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_year_category_id(id, word,inv_position,frequency,category,year) values (?,?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j,1,category,yearInt));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.word_inv_pos_year_category_id SET frequency = ? WHERE word=? AND inv_position=? AND category=? AND year=?");
								session.execute(statement.bind(row.getInt("frequency") + 1, words[j],words.length-1-j,category,yearInt));
								
								statement = session
										.prepare("DELETE FROM corpus.word_inv_pos_year_category_frequency WHERE word=? AND inv_position=? AND frequency = ? AND category=? AND year=?");
								session.execute(statement.bind( words[j],words.length-1-j, row.getInt("frequency"),category,yearInt));
								
								statement = session
										.prepare("INSERT INTO corpus.word_inv_pos_year_category_frequency(id, word, inv_position, frequency,category,year) values (?,?,?,?,?,?)");
								session.execute(statement.bind(wordcount, words[j],words.length-1-j, row.getInt("frequency") + 1,category,yearInt));

							}
							
						    
							wordcount++;
						}
					}

					for (int j = 0; j < words.length - 1; j++) {
						if (words[j].length() > 0 && words[j + 1].length() > 0) {

							statement = session
									.prepare("select * from corpus.bigram_time_category_frequency WHERE word1=? AND word2=? AND year=? AND category=?");
							ResultSet results = session.execute(statement.bind(
									words[j], words[j + 1], yearInt, category));
							// System.out.println("3 right");
							Row row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.bigram_time_category_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],
										words[j + 1], yearInt, category, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.bigram_time_category_ordered_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j], words[j+1],
										yearInt, category, 1));
								
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.bigram_time_category_frequency SET frequency = ? WHERE word1=? AND word2=? AND year=? AND category=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1], yearInt, category));
								
								statement = session
										.prepare("DELETE FROM corpus.bigram_time_category_ordered_frequency WHERE word1=? AND word2=? AND year=? AND category=? AND frequency = ?");
								session.execute(statement.bind( words[j],words[j+1],
										yearInt, category, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.bigram_time_category_ordered_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j], words[j+1],
										yearInt, category, row.getInt("frequency") + 1));
							}

							// ////////////////////////////////////

							statement = session
									.prepare("select * from corpus.bigram_frequency WHERE word1=? AND word2=?");
							results = session.execute(statement.bind(words[j],
									words[j + 1]));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.bigram_frequency(id, word1,word2,frequency) values (?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],
										words[j + 1], 1));
								// System.out.println("4b right");
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.bigram_frequency SET frequency = ? WHERE word1=? AND word2=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1]));
								// System.out.println("4a right");
							}

							// //////////////////////////////////////////

							statement = session
									.prepare("select * from corpus.bigram_time_frequency WHERE word1=? AND word2=? AND year=?");
							results = session.execute(statement.bind(words[j],
									words[j + 1], yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.bigram_time_frequency(id, word1,word2,year,frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],
										words[j + 1], yearInt, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.bigram_time_ordered_frequency(id, word1,word2, year, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],words[j+1],
										yearInt,  1));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.bigram_time_frequency SET frequency = ? WHERE word1=? AND word2=? AND year=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1], yearInt));
								
								statement = session
										.prepare("DELETE FROM corpus.bigram_time_ordered_frequency WHERE word1=? AND word2=? AND year=?  AND frequency = ?");
								session.execute(statement.bind( words[j],words[j+1],
										yearInt,  row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.bigram_time_ordered_frequency(id, word1,word2, year, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],words[j+1],
										yearInt, row.getInt("frequency") + 1));

							}

							// ////////////////////////////////////////////////////

							statement = session
									.prepare("select * from corpus.bigram_category_frequency WHERE word1=? AND word2=? AND category=?");
							results = session.execute(statement.bind(words[j],
									words[j + 1], category));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.bigram_category_frequency(id, word1,word2,category,frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],
										words[j + 1], category, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.bigram_category_ordered_frequency(id, word1,word2, category, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],words[j+1],
										category, 1));

							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.bigram_category_frequency SET frequency = ? WHERE word1=? AND word2=? and category=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1], category));
								
								statement = session
										.prepare("DELETE FROM corpus.bigram_category_ordered_frequency WHERE word1=? AND word2=? AND category=? AND frequency = ?");
								session.execute(statement.bind( words[j],words[j+1],
										category, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.bigram_category_ordered_frequency(id, word1,word2, category, frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(bigramcount, words[j],words[j+1],
										category, row.getInt("frequency") + 1));

							}
							
							statement= session.prepare(
									"INSERT INTO corpus.bigram_usage (id, word1,word2, sentence, postname, date, url,author) values (?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(bigramcount,words[j],words[j+1],sentences[i],topic,date,link,author));
						
						    statement= session.prepare(
									"INSERT INTO corpus.bigram_year_usage (id, word1, word2, sentence, postname, year, date, url, author) values (?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(bigramcount,words[j],words[j+1],sentences[i],topic,yearInt,date,link,author));
						    
						    statement= session.prepare(
									"INSERT INTO corpus.bigram_category_usage (id, word1,word2, sentence, postname, category, date, url, author) values (?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(bigramcount,words[j],words[j+1],sentences[i],topic,category,date,link,author));
						    
						    statement= session.prepare(
									"INSERT INTO corpus.bigram_year_category_usage (id, word1,word2, sentence, postname, year, category, date, url, author) values (?,?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(bigramcount,words[j],words[j+1],sentences[i],topic,yearInt, category,date,link,author));
							
						    
							bigramcount++;						
							}
					}

					for (int j = 0; j < words.length - 2; j++) {
						if(words[j].length()>0 && words[j+1].length()>0 && words[j+2].length()>0){
							
							statement = session
									.prepare("select * from corpus.trigram_time_category_frequency WHERE word1=? AND word2=? AND word3=? AND year=? AND category=?");
							ResultSet results = session.execute(statement.bind(
									words[j], words[j + 1],words[j+2], yearInt, category));
							// System.out.println("3 right");
							Row row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.trigram_time_category_frequency(id, word1, word2, word3, year, category, frequency) values (?,?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],	words[j + 1], words[j+2],yearInt, category, 1));
								
								statement = session
										.prepare("INSERT INTO corpus.trigram_time_category_ordered_frequency(id, word1, word2, word3, year, category, frequency) values (?,?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j], words[j+1],words[j+1],
										yearInt, category, 1));
								
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.trigram_time_category_frequency SET frequency = ? WHERE word1=? AND word2=? AND word3=? AND year=? AND category=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1],words[j+2], yearInt, category));
								
								statement = session
										.prepare("DELETE FROM corpus.trigram_time_category_ordered_frequency WHERE word1=? AND word2=? AND word3=? AND year=? AND category=? AND frequency = ?");
								session.execute(statement.bind( words[j],words[j+1],words[j+2],
										yearInt, category, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.trigram_time_category_ordered_frequency(id, word1, word2, word3, year, category, frequency) values (?,?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j], words[j+1],words[j+2],
										yearInt, category, row.getInt("frequency") + 1));
								
								
							}
							
							///////////////////////////////////////////////
							
							statement = session
									.prepare("select * from corpus.trigram_frequency WHERE word1=? AND word2=? AND word3=?");
							results = session.execute(statement.bind(words[j],
									words[j + 1],words[j+2]));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.trigram_frequency(id, word1,word2,word3,frequency) values (?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],
										words[j + 1],words[j+2], 1));
								// System.out.println("4b right");
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.trigram_frequency SET frequency = ? WHERE word1=? AND word2=? AND word3=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1],words[j+2]));
								// System.out.println("4a right");
							}
							
							///////////////////////////////////////////
							
							statement = session
									.prepare("select * from corpus.trigram_time_frequency WHERE word1=? AND word2=? AND word3=? AND year=?");
							results = session.execute(statement.bind(words[j],
									words[j + 1], words[j+2], yearInt));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.trigram_time_frequency(id, word1,word2,word3,year,frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],
										words[j + 1], words[j+2], yearInt, 1));

								statement = session
										.prepare("INSERT INTO corpus.trigram_time_ordered_frequency(id, word1,word2,word3, year, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],words[j+1],words[j+2],
										yearInt,  1));
								
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.trigram_time_frequency SET frequency = ? WHERE word1=? AND word2=? AND word3=? AND year=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1], words[j+2], yearInt));

								statement = session
										.prepare("DELETE FROM corpus.trigram_time_ordered_frequency WHERE word1=? AND word2=? AND word3=? AND year=?  AND frequency = ?");
								session.execute(statement.bind( words[j],words[j+1],words[j+2],
										yearInt,  row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.trigram_time_ordered_frequency(id, word1,word2,word3, year, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],words[j+1],words[j+2],
										yearInt, row.getInt("frequency") + 1));
								
							}
							
							///////////////////////////
							
							statement = session
									.prepare("select * from corpus.trigram_category_frequency WHERE word1=? AND word2=? AND word3=? AND category=?");
							results = session.execute(statement.bind(words[j],
									words[j + 1],words[j+2], category));
							// System.out.println("3 right");
							row = results.one();
							if (row == null) {
								// System.out.println("4b");
								statement = session
										.prepare("INSERT INTO corpus.trigram_category_frequency(id, word1,word2,word3,category,frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],
										words[j + 1], words[j+2], category, 1));

								statement = session
										.prepare("INSERT INTO corpus.trigram_category_ordered_frequency(id, word1,word2,word3, category, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],words[j+1],words[j+2],
										category, 1));
								
							} else {
								// System.out.println("4a");
								statement = session
										.prepare("UPDATE corpus.trigram_category_frequency SET frequency = ? WHERE word1=? AND word2=? AND word3=? and category=?");
								session.execute(statement.bind(
										row.getInt("frequency") + 1, words[j],
										words[j + 1], words[j+2], category));

								statement = session
										.prepare("DELETE FROM corpus.trigram_category_ordered_frequency WHERE word1=? AND word2=? AND word3=? AND category=? AND frequency = ?");
								session.execute(statement.bind( words[j],words[j+1],words[j+2],
										category, row.getInt("frequency")));
								
								statement = session
										.prepare("INSERT INTO corpus.trigram_category_ordered_frequency(id, word1,word2,word3, category, frequency) values (?,?,?,?,?,?)");
								session.execute(statement.bind(trigramcount, words[j],words[j+1],words[j+2],
										category, row.getInt("frequency") + 1));
								
							}
							
							statement= session.prepare(
									"INSERT INTO corpus.trigram_usage (id, word1,word2,word3, sentence, postname, date, url,author) values (?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(trigramcount,words[j],words[j+1],words[j+2],sentences[i],topic,date,link,author));
						
						    statement= session.prepare(
									"INSERT INTO corpus.trigram_year_usage (id, word1, word2,word3, sentence, postname, year, date, url, author) values (?,?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(trigramcount,words[j],words[j+1],words[j+2],sentences[i],topic,yearInt,date,link,author));
						    
						    statement= session.prepare(
									"INSERT INTO corpus.trigram_category_usage (id, word1,word2,word3, sentence, postname, category, date, url, author) values (?,?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(trigramcount,words[j],words[j+1],words[j+2],sentences[i],topic,category,date,link,author));
						    
						    statement= session.prepare(
									"INSERT INTO corpus.trigram_year_category_usage (id, word1,word2,word3, sentence, postname, year, category, date, url, author) values (?,?,?,?,?,?,?,?,?,?,?)");
						    session.execute(statement.bind(trigramcount,words[j],words[j+1],words[j+2],sentences[i],topic,yearInt, category,date,link,author));
							
							
							trigramcount++;
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
		CassandraClient cl = new CassandraClient();
		cl.connect("127.0.0.1");
		cl.feed("/home/chamila/semester7/fyp/20_Million_Words/out/1.xml");
		System.out.println("dddddddd");
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
