/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.sinmin.corpus.cassandra;

import java.util.Date;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

public class BigramInsert extends Thread {
    final static Logger logger = Logger.getLogger(BigramInsert.class);

    private Cluster cluster;
    private Session session;
    private PreparedStatement statement;
    private String word1, word2, category, sentence, topic, author, link;
    private long bigramcount;
    private int yearInt, j, senLen;
    private Date date;

    public void connect(String node) {

        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        session = cluster.connect();
    }

    public void close() {
        cluster.close();
    }

    public void run() {
        dbOperation();
    }

    public void setParams(String word1, String word2, long bigramcount, int yearInt, String category, String sentence, int j, int senLen, String topic, String author, String link, Date date) {
        this.word1 = word1;
        this.word2 = word2;
        this.yearInt = yearInt;
        this.author = author;
        this.sentence = sentence;
        this.bigramcount = bigramcount;
        this.category = category;
        this.j = j;
        this.senLen = senLen;
        this.topic = topic;
        this.link = link;
        this.date = date;

    }

    public void dbOperation() {

        statement = session
                .prepare("select * from corpus.bigram_time_category_frequency WHERE word1=? AND word2=? AND year=? AND category=?");
        ResultSet results = session.execute(statement.bind(
                word1, word2, yearInt, category));
        Row row = results.one();
        if (row == null) {
            statement = session
                    .prepare("INSERT INTO corpus.bigram_time_category_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1,
                    word2, yearInt, category, 1));

            statement = session
                    .prepare("INSERT INTO corpus.bigram_time_category_ordered_frequency(id, word1, word2, year, category, frequency) values (?,?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1, word2,
                    yearInt, category, 1));

        } else {
            statement = session
                    .prepare("UPDATE corpus.bigram_time_category_frequency SET frequency = ? WHERE word1=? AND word2=? AND year=? AND category=?");
            session.execute(statement.bind(
                    row.getInt("frequency") + 1, word1,
                    word2, yearInt, category));

            statement = session
                    .prepare("DELETE FROM corpus.bigram_time_category_ordered_frequency WHERE word1=? AND word2=? AND year=? AND category=? AND frequency = ?");
            session.execute(statement.bind(word1, word2,
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
        row = results.one();
        if (row == null) {
            statement = session
                    .prepare("INSERT INTO corpus.bigram_frequency(id, word1,word2,frequency) values (?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1,
                    word2, 1));
        } else {
            statement = session
                    .prepare("UPDATE corpus.bigram_frequency SET frequency = ? WHERE word1=? AND word2=?");
            session.execute(statement.bind(
                    row.getInt("frequency") + 1, word1,
                    word2));
        }

        // //////////////////////////////////////////

        statement = session
                .prepare("select * from corpus.bigram_time_frequency WHERE word1=? AND word2=? AND year=?");
        results = session.execute(statement.bind(word1,
                word2, yearInt));
        row = results.one();
        if (row == null) {
            statement = session
                    .prepare("INSERT INTO corpus.bigram_time_frequency(id, word1,word2,year,frequency) values (?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1,
                    word2, yearInt, 1));

            statement = session
                    .prepare("INSERT INTO corpus.bigram_time_ordered_frequency(id, word1,word2, year, frequency) values (?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1, word2,
                    yearInt, 1));

        } else {
            statement = session
                    .prepare("UPDATE corpus.bigram_time_frequency SET frequency = ? WHERE word1=? AND word2=? AND year=?");
            session.execute(statement.bind(
                    row.getInt("frequency") + 1, word1,
                    word2, yearInt));

            statement = session
                    .prepare("DELETE FROM corpus.bigram_time_ordered_frequency WHERE word1=? AND word2=? AND year=?  AND frequency = ?");
            session.execute(statement.bind(word1, word2,
                    yearInt, row.getInt("frequency")));

            statement = session
                    .prepare("INSERT INTO corpus.bigram_time_ordered_frequency(id, word1,word2, year, frequency) values (?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1, word2,
                    yearInt, row.getInt("frequency") + 1));

        }

        // ////////////////////////////////////////////////////

        statement = session
                .prepare("select * from corpus.bigram_category_frequency WHERE word1=? AND word2=? AND category=?");
        results = session.execute(statement.bind(word1,
                word2, category));
        row = results.one();
        if (row == null) {
            statement = session
                    .prepare("INSERT INTO corpus.bigram_category_frequency(id, word1,word2,category,frequency) values (?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1,
                    word2, category, 1));

            statement = session
                    .prepare("INSERT INTO corpus.bigram_category_ordered_frequency(id, word1,word2, category, frequency) values (?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1, word2,
                    category, 1));

        } else {
            statement = session
                    .prepare("UPDATE corpus.bigram_category_frequency SET frequency = ? WHERE word1=? AND word2=? and category=?");
            session.execute(statement.bind(
                    row.getInt("frequency") + 1, word1,
                    word2, category));

            statement = session
                    .prepare("DELETE FROM corpus.bigram_category_ordered_frequency WHERE word1=? AND word2=? AND category=? AND frequency = ?");
            session.execute(statement.bind(word1, word2,
                    category, row.getInt("frequency")));

            statement = session
                    .prepare("INSERT INTO corpus.bigram_category_ordered_frequency(id, word1,word2, category, frequency) values (?,?,?,?,?)");
            session.execute(statement.bind(bigramcount, word1, word2,
                    category, row.getInt("frequency") + 1));

        }

        statement = session.prepare(
                "INSERT INTO corpus.bigram_usage (id, word1,word2, sentence, postname, date, url,author) values (?,?,?,?,?,?,?,?)");
        session.execute(statement.bind(bigramcount, word1, word2, sentence, topic, date, link, author));

        statement = session.prepare(
                "INSERT INTO corpus.bigram_year_usage (id, word1, word2, sentence, postname, year, date, url, author) values (?,?,?,?,?,?,?,?,?)");
        session.execute(statement.bind(bigramcount, word1, word2, sentence, topic, yearInt, date, link, author));

        statement = session.prepare(
                "INSERT INTO corpus.bigram_category_usage (id, word1,word2, sentence, postname, category, date, url, author) values (?,?,?,?,?,?,?,?,?)");
        session.execute(statement.bind(bigramcount, word1, word2, sentence, topic, category, date, link, author));

        statement = session.prepare(
                "INSERT INTO corpus.bigram_year_category_usage (id, word1,word2, sentence, postname, year, category, date, url, author) values (?,?,?,?,?,?,?,?,?,?)");
        session.execute(statement.bind(bigramcount, word1, word2, sentence, topic, yearInt, category, date, link, author));


        bigramcount++;
        close();
        logger.info("bclose");

    }

}
