package com.sinmin.corpus.cassandra;

import java.util.Date;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class BigramInsert extends Thread{

	private Cluster cluster;
	private Session session;
	PreparedStatement statement;
	public void connect(String node) {
		
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
//		System.out.printf("Connected to cluster: %s\n",
//				metadata.getClusterName());
//		for (Host host : metadata.getAllHosts()) {
//			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
//					host.getDatacenter(), host.getAddress(), host.getRack());
//		}
		session = cluster.connect();
	}
	
	public void close()  
	   {  
	      cluster.close();  
	   }  
	
	public void run() {
		dbOperation();
	}
	
	String word1;
	String word2;long bigramcount;
	int yearInt;
	String category;
	String sentence;
	int j;
	int senLen;
	String topic;
	String author;
	String link;
	Date date;
	
	public void setParams(String word1,String word2,long bigramcount, int yearInt, String category, String sentence,int j, int senLen, String topic, String author, String link, Date date){
		this.word1=word1;
		this.word2=word2;
		this.yearInt=yearInt;
		this.author=author;
		this.sentence=sentence;
		this.bigramcount=bigramcount;
		this.category=category;
		this.j=j;
		this.senLen=senLen;this.topic=topic;
		this.link=link;
		this.date=date;
				
	}
	
	public void dbOperation(){
		
        //System.out.println("bigram   " +word1 + "   " + word2);

		statement = session
				.prepare("select * from corpus.bigram_time_category_frequency WHERE word1=? AND word2=? AND year=? AND category=?");
		ResultSet results = session.execute(statement.bind(
				word1, word2, yearInt, category));
		// System.out.println("3 right");
		Row row = results.one();
		if (row == null) {
			// System.out.println("4b");
			statement = session
					.prepare("INSERT INTO corpus.bigram_time_category_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,
					word2, yearInt, category, 1));
			
			statement = session
					.prepare("INSERT INTO corpus.bigram_time_category_ordered_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1, word2,
					yearInt, category, 1));
			
		} else {
			// System.out.println("4a");
			statement = session
					.prepare("UPDATE corpus.bigram_time_category_frequency SET frequency = ? WHERE word1=? AND word2=? AND year=? AND category=?");
			session.execute(statement.bind(
					row.getInt("frequency") + 1, word1,
					word2, yearInt, category));
			
			statement = session
					.prepare("DELETE FROM corpus.bigram_time_category_ordered_frequency WHERE word1=? AND word2=? AND year=? AND category=? AND frequency = ?");
			session.execute(statement.bind( word1,word2,
					yearInt, category, row.getInt("frequency")));
			
			statement = session
					.prepare("INSERT INTO corpus.bigram_time_category_ordered_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1, word2,
					yearInt, category, row.getInt("frequency") + 1));
		}

		// ////////////////////////////////////

		statement = session
				.prepare("select * from corpus.bigram_frequency WHERE word1=? AND word2=?");
		results = session.execute(statement.bind(word1,
				word2));
		// System.out.println("3 right");
		row = results.one();
		if (row == null) {
			// System.out.println("4b");
			statement = session
					.prepare("INSERT INTO corpus.bigram_frequency(id, word1,word2,frequency) values (?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,
					word2, 1));
			// System.out.println("4b right");
		} else {
			// System.out.println("4a");
			statement = session
					.prepare("UPDATE corpus.bigram_frequency SET frequency = ? WHERE word1=? AND word2=?");
			session.execute(statement.bind(
					row.getInt("frequency") + 1, word1,
					word2));
			// System.out.println("4a right");
		}

		// //////////////////////////////////////////

		statement = session
				.prepare("select * from corpus.bigram_time_frequency WHERE word1=? AND word2=? AND year=?");
		results = session.execute(statement.bind(word1,
				word2, yearInt));
		// System.out.println("3 right");
		row = results.one();
		if (row == null) {
			// System.out.println("4b");
			statement = session
					.prepare("INSERT INTO corpus.bigram_time_frequency(id, word1,word2,year,frequency) values (?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,
					word2, yearInt, 1));
			
			statement = session
					.prepare("INSERT INTO corpus.bigram_time_ordered_frequency(id, word1,word2, year, frequency) values (?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,word2,
					yearInt,  1));

		} else {
			// System.out.println("4a");
			statement = session
					.prepare("UPDATE corpus.bigram_time_frequency SET frequency = ? WHERE word1=? AND word2=? AND year=?");
			session.execute(statement.bind(
					row.getInt("frequency") + 1, word1,
					word2, yearInt));
			
			statement = session
					.prepare("DELETE FROM corpus.bigram_time_ordered_frequency WHERE word1=? AND word2=? AND year=?  AND frequency = ?");
			session.execute(statement.bind( word1,word2,
					yearInt,  row.getInt("frequency")));
			
			statement = session
					.prepare("INSERT INTO corpus.bigram_time_ordered_frequency(id, word1,word2, year, frequency) values (?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,word2,
					yearInt, row.getInt("frequency") + 1));

		}

		// ////////////////////////////////////////////////////

		statement = session
				.prepare("select * from corpus.bigram_category_frequency WHERE word1=? AND word2=? AND category=?");
		results = session.execute(statement.bind(word1,
				word2, category));
		// System.out.println("3 right");
		row = results.one();
		if (row == null) {
			// System.out.println("4b");
			statement = session
					.prepare("INSERT INTO corpus.bigram_category_frequency(id, word1,word2,category,frequency) values (?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,
					word2, category, 1));
			
			statement = session
					.prepare("INSERT INTO corpus.bigram_category_ordered_frequency(id, word1,word2, category, frequency) values (?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,word2,
					category, 1));

		} else {
			// System.out.println("4a");
			statement = session
					.prepare("UPDATE corpus.bigram_category_frequency SET frequency = ? WHERE word1=? AND word2=? and category=?");
			session.execute(statement.bind(
					row.getInt("frequency") + 1, word1,
					word2, category));
			
			statement = session
					.prepare("DELETE FROM corpus.bigram_category_ordered_frequency WHERE word1=? AND word2=? AND category=? AND frequency = ?");
			session.execute(statement.bind( word1,word2,
					category, row.getInt("frequency")));
			
			statement = session
					.prepare("INSERT INTO corpus.bigram_category_ordered_frequency(id, word1,word2, category, frequency) values (?,?,?,?,?)");
			session.execute(statement.bind(bigramcount, word1,word2,
					category, row.getInt("frequency") + 1));

		}
		
		statement= session.prepare(
				"INSERT INTO corpus.bigram_usage (id, word1,word2, sentence, postname, date, url,author) values (?,?,?,?,?,?,?,?)");
	    session.execute(statement.bind(bigramcount,word1,word2,sentence,topic,date,link,author));
	
	    statement= session.prepare(
				"INSERT INTO corpus.bigram_year_usage (id, word1, word2, sentence, postname, year, date, url, author) values (?,?,?,?,?,?,?,?,?)");
	    session.execute(statement.bind(bigramcount,word1,word2,sentence,topic,yearInt,date,link,author));
	    
	    statement= session.prepare(
				"INSERT INTO corpus.bigram_category_usage (id, word1,word2, sentence, postname, category, date, url, author) values (?,?,?,?,?,?,?,?,?)");
	    session.execute(statement.bind(bigramcount,word1,word2,sentence,topic,category,date,link,author));
	    
	    statement= session.prepare(
				"INSERT INTO corpus.bigram_year_category_usage (id, word1,word2, sentence, postname, year, category, date, url, author) values (?,?,?,?,?,?,?,?,?,?)");
	    session.execute(statement.bind(bigramcount,word1,word2,sentence,topic,yearInt, category,date,link,author));
		
	    
		bigramcount++;						
		close();
		System.out.println("bclose");
		
	}
	
}
