package com.sinmin.corpus.cassandra;

import java.util.Date;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class WordInsert extends Thread {
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
	
	String word;long wordcount;
	int yearInt;
	String category;
	String sentence;
	int j;
	int senLen;
	String topic;
	String author;
	String link;
	Date date;
	
	public void setParams(String word,long wordcount, int yearInt, String category, String sentence,int j, int senLen, String topic, String author, String link, Date date){
		this.word=word;
		this.yearInt=yearInt;
		this.author=author;
		this.sentence=sentence;
		this.wordcount=wordcount;
		this.category=category;
		this.j=j;
		this.senLen=senLen;this.topic=topic;
		this.link=link;
		this.date=date;
				
	}
	
	public void dbOperation(){

		//System.out.println("word    " + word);
		if (word.length() > 0) {
			statement = session
					.prepare("select * from corpus.word_time_category_frequency WHERE word=? AND year=? AND category=?");
			ResultSet results = session.execute(statement.bind(
					word, yearInt, category));
			// System.out.println("3 right");
			Row row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_time_category_frequency(id, word, year, category, frequency) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						yearInt, category, 1));
				
				statement = session
						.prepare("INSERT INTO corpus.word_time_category_ordered_frequency(id, word, year, category, frequency) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						yearInt, category, 1));
				// System.out.println("4b right");
			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_time_category_frequency SET frequency = ? WHERE word=? AND year=? AND category=?");
				session.execute(statement.bind(
						row.getInt("frequency") + 1, word,
						yearInt, category));
				
				statement = session
						.prepare("DELETE FROM corpus.word_time_category_ordered_frequency WHERE word=? AND year=? AND category=? AND frequency = ?");
				session.execute(statement.bind( word,
						yearInt, category, row.getInt("frequency")));
				
				statement = session
						.prepare("INSERT INTO corpus.word_time_category_ordered_frequency(id, word, year, category, frequency) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						yearInt, category, row.getInt("frequency") + 1));
				// System.out.println("4a right");
			}
			
			// //////////////////////////////////////////////////////
			
			statement = session
					.prepare("select * from corpus.word_frequency WHERE word=?");
			results = session.execute(statement.bind(word));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_frequency(id, word,frequency) values (?,?,?)");
				session.execute(statement.bind(wordcount, word,
						1));
				
				// System.out.println("4b right");
			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_frequency SET frequency = ? WHERE word=? ");
				session.execute(statement.bind(
						row.getInt("frequency") + 1, word));
				
				// System.out.println("4a right");
			}

			// ///////////////////////////////////////////////////////////////

			statement = session
					.prepare("select * from corpus.word_time_frequency WHERE word=? AND year=?");
			results = session.execute(statement.bind(word,
					yearInt));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_time_frequency(id, word,year,frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						yearInt, 1));
				
				statement = session
						.prepare("INSERT INTO corpus.word_time_ordered_frequency(id, word, year, frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						yearInt,  1));
				
				// System.out.println("4b right");
			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_time_frequency SET frequency = ? WHERE word=? and year=?");
				session.execute(statement.bind(
						row.getInt("frequency") + 1, word,
						yearInt));
				
				statement = session
						.prepare("DELETE FROM corpus.word_time_ordered_frequency WHERE word=? AND year=?  AND frequency = ?");
				session.execute(statement.bind( word,
						yearInt,  row.getInt("frequency")));
				
				statement = session
						.prepare("INSERT INTO corpus.word_time_ordered_frequency(id, word, year, frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						yearInt, row.getInt("frequency") + 1));
			}

			// ///////////////////////////////////////////

			statement = session
					.prepare("select * from corpus.word_category_frequency WHERE word=? AND category=?");
			results = session.execute(statement.bind(word,
					category));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_category_frequency(id, word,category,frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						category, 1));
				
				statement = session
						.prepare("INSERT INTO corpus.word_category_ordered_frequency(id, word, category, frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						category, 1));
				// System.out.println("4b right");
			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_category_frequency SET frequency = ? WHERE word=? and category=?");
				session.execute(statement.bind(
						row.getInt("frequency") + 1, word,
						category));
				
				statement = session
						.prepare("DELETE FROM corpus.word_category_ordered_frequency WHERE word=?  AND category=? AND frequency = ?");
				session.execute(statement.bind( word,
						category, row.getInt("frequency")));
				
				statement = session
						.prepare("INSERT INTO corpus.word_category_ordered_frequency(id, word, category, frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,
						category, row.getInt("frequency") + 1));
			}
			
			statement= session.prepare(
					"INSERT INTO corpus.word_usage (id, word, sentence, postname, date, url,author) values (?,?,?,?,?,?,?)");
		    session.execute(statement.bind(wordcount,word,sentence,topic,date,link,author));
		
		    statement= session.prepare(
					"INSERT INTO corpus.word_year_usage (id, word, sentence, postname, year, date, url, author) values (?,?,?,?,?,?,?,?)");
		    session.execute(statement.bind(wordcount,word,sentence,topic,yearInt,date,link,author));
		    
		    statement= session.prepare(
					"INSERT INTO corpus.word_category_usage (id, word, sentence, postname, category, date, url, author) values (?,?,?,?,?,?,?,?)");
		    session.execute(statement.bind(wordcount,word,sentence,topic,category,date,link,author));
		    
		    statement= session.prepare(
					"INSERT INTO corpus.word_year_category_usage (id, word, sentence, postname, year, category, date, url, author) values (?,?,?,?,?,?,?,?,?)");
		    session.execute(statement.bind(wordcount,word,sentence,topic,yearInt, category,date,link,author));
			
		    //////////////////////////////////
		    
		    statement = session
					.prepare("select * from corpus.word_pos_id WHERE word=? AND position=?");
			results = session.execute(statement.bind(word,j));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_pos_frequency(id, word,position,frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_id(id, word,position,frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_pos_id SET frequency = ? WHERE word=? AND position=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,j));
				
				statement = session
						.prepare("DELETE FROM corpus.word_pos_frequency WHERE word=? AND position=? AND frequency = ?");
				session.execute(statement.bind( word,j, row.getInt("frequency")));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_frequency(id, word, position, frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j, row.getInt("frequency") + 1));

			}
			
			statement = session
					.prepare("select * from corpus.word_inv_pos_id WHERE word=? AND inv_position=?");
			results = session.execute(statement.bind(word,senLen-1-j));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_frequency(id, word,inv_position,frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_id(id, word,inv_position,frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_inv_pos_id SET frequency = ? WHERE word=? AND inv_position=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,senLen-1-j));
				
				statement = session
						.prepare("DELETE FROM corpus.word_inv_pos_frequency WHERE word=? AND inv_position=? AND frequency = ?");
				session.execute(statement.bind( word,senLen-1-j, row.getInt("frequency")));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_frequency(id, word, inv_position, frequency) values (?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j, row.getInt("frequency") + 1));

			}
		    
		    //////////////////////////////////////////////////////////////////
		    
			statement = session
					.prepare("select * from corpus.word_pos_year_id WHERE word=? AND position=? AND year=?");
			results = session.execute(statement.bind(word,j,yearInt));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_pos_year_frequency(id, word,position,frequency,year) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1,yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_year_id(id, word,position,frequency,year) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1,yearInt));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_pos_year_id SET frequency = ? WHERE word=? AND position=? AND year=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,j,yearInt));
				
				statement = session
						.prepare("DELETE FROM corpus.word_pos_year_frequency WHERE word=? AND position=? AND frequency = ? AND year=?");
				session.execute(statement.bind( word,j, row.getInt("frequency"),yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_year_frequency(id, word, position, frequency,year) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j, row.getInt("frequency") + 1,yearInt));

			}
			
			statement = session
					.prepare("select * from corpus.word_inv_pos_year_id WHERE word=? AND inv_position=? AND year=?");
			results = session.execute(statement.bind(word,senLen-1-j,yearInt));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_year_frequency(id, word,inv_position,frequency,year) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1,yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_year_id(id, word,inv_position,frequency,year) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1,yearInt));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_inv_pos_year_id SET frequency = ? WHERE word=? AND inv_position=? AND year=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,senLen-1-j,yearInt));
				
				statement = session
						.prepare("DELETE FROM corpus.word_inv_pos_year_frequency WHERE word=? AND inv_position=? AND frequency = ? AND year=?");
				session.execute(statement.bind( word,senLen-1-j, row.getInt("frequency"),yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_year_frequency(id, word, inv_position, frequency,year) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j, row.getInt("frequency") + 1,yearInt));

			}
			
			
			/////////////////////////////////////////////////////////////////////////
			
			statement = session
					.prepare("select * from corpus.word_pos_category_id WHERE word=? AND position=? AND category=?");
			results = session.execute(statement.bind(word,j,category));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_pos_category_frequency(id, word,position,frequency,category) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1,category));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_category_id(id, word,position,frequency,category) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1,category));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_pos_category_id SET frequency = ? WHERE word=? AND position=? AND category=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,j,category));
				
				statement = session
						.prepare("DELETE FROM corpus.word_pos_category_frequency WHERE word=? AND position=? AND frequency = ? AND category=?");
				session.execute(statement.bind( word,j, row.getInt("frequency"),category));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_category_frequency(id, word, position, frequency,category) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j, row.getInt("frequency") + 1,category));

			}
			
			statement = session
					.prepare("select * from corpus.word_inv_pos_category_id WHERE word=? AND inv_position=? AND category=?");
			results = session.execute(statement.bind(word,senLen-1-j,category));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_category_frequency(id, word,inv_position,frequency,category) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1,category));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_category_id(id, word,inv_position,frequency,category) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1,category));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_inv_pos_category_id SET frequency = ? WHERE word=? AND inv_position=? AND category=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,senLen-1-j,category));
				
				statement = session
						.prepare("DELETE FROM corpus.word_inv_pos_category_frequency WHERE word=? AND inv_position=? AND frequency = ? AND category=?");
				session.execute(statement.bind( word,senLen-1-j, row.getInt("frequency"),category));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_category_frequency(id, word, inv_position, frequency,category) values (?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j, row.getInt("frequency") + 1,category));

			}
			
			
			//////////////////////////////////////////////////////////
			
			statement = session
					.prepare("select * from corpus.word_pos_year_category_id WHERE word=? AND position=? AND category=? AND year=?");
			results = session.execute(statement.bind(word,j,category,yearInt));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_pos_year_category_frequency(id, word,position,frequency,category,year) values (?,?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1,category,yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_year_category_id(id, word,position,frequency,category,year) values (?,?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j,1,category,yearInt));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_pos_year_category_id SET frequency = ? WHERE word=? AND position=? AND category=? AND year=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,j,category,yearInt));
				
				statement = session
						.prepare("DELETE FROM corpus.word_pos_year_category_frequency WHERE word=? AND position=? AND frequency = ? AND category=? AND year=?");
				session.execute(statement.bind( word,j, row.getInt("frequency"),category,yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_pos_year_category_frequency(id, word, position, frequency,category,year) values (?,?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,j, row.getInt("frequency") + 1,category,yearInt));

			}
			
			statement = session
					.prepare("select * from corpus.word_inv_pos_year_category_id WHERE word=? AND inv_position=? AND category=? AND year=?");
			results = session.execute(statement.bind(word,senLen-1-j,category,yearInt));
			// System.out.println("3 right");
			row = results.one();
			if (row == null) {
				// System.out.println("4b");
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_year_category_frequency(id, word,inv_position,frequency,category,year) values (?,?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1,category,yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_year_category_id(id, word,inv_position,frequency,category,year) values (?,?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j,1,category,yearInt));

			} else {
				// System.out.println("4a");
				statement = session
						.prepare("UPDATE corpus.word_inv_pos_year_category_id SET frequency = ? WHERE word=? AND inv_position=? AND category=? AND year=?");
				session.execute(statement.bind(row.getInt("frequency") + 1, word,senLen-1-j,category,yearInt));
				
				statement = session
						.prepare("DELETE FROM corpus.word_inv_pos_year_category_frequency WHERE word=? AND inv_position=? AND frequency = ? AND category=? AND year=?");
				session.execute(statement.bind( word,senLen-1-j, row.getInt("frequency"),category,yearInt));
				
				statement = session
						.prepare("INSERT INTO corpus.word_inv_pos_year_category_frequency(id, word, inv_position, frequency,category,year) values (?,?,?,?,?,?)");
				session.execute(statement.bind(wordcount, word,senLen-1-j, row.getInt("frequency") + 1,category,yearInt));

			}
			
		    
			wordcount++;
		}
	close();
	System.out.println("wclose");
	}
}
	

