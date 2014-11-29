package com.sinmin.corpus.bean;

import java.io.Serializable;

/**
 * Created by dimuthuupeksha on 11/28/14.
 */
public class DFile implements Serializable{
    private String name;
    private Folder root;

    public DFile(String name,Folder root){
        this.name = name;
        this.root = root;
    }

    public String getName() {
        return name;
    }
}
