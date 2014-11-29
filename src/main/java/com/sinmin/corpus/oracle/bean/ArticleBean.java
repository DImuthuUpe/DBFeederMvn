package com.sinmin.corpus.oracle.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dimuthuupeksha on 8/29/14.
 */
public class ArticleBean {
    public long id;
    public String day="";
    public String month="";
    public String year="";
    public String topic="";
    public String author="";
    public String category="";
    public String subCat1="";
    public String link ="";

    public List<Long> sentenceIds = new ArrayList<>();
    public List<SentenceBean> sentences = new ArrayList<>();
}
