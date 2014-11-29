package com.sinmin.corpus.bean;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by dimuthuupeksha on 11/28/14.
 */
public class Folder implements Serializable{
    private HashSet<DFile> files;
    private HashSet<Folder> folders;
    private String name;
    private Folder root;

    public Folder(String name, Folder root){
        this.name= name;
        files = new HashSet<>();
        folders = new HashSet<>();
        this.root = root;
    }

    public void addFile(DFile file){
        files.add(file);
    }
    public void addFolder(Folder folder){
        folders.add(folder);
    }

    public String getName(){
        return name;
    }

    public DFile getFile(String name){
        Iterator<DFile> itFiles = files.iterator();
        while(itFiles.hasNext()){
            DFile f = itFiles.next();
            if(f.getName().equals(name)){
                return f;
            }
        }
        return null;
    }

    public Folder getFolder(String name){
        Iterator<Folder> itFolders = folders.iterator();
        while(itFolders.hasNext()){
            Folder f = itFolders.next();
            if(f.getName().equals(name)){
                return f;
            }
        }
        return null;
    }


    public boolean containsFile(String file){
        Iterator<DFile> itFiles = files.iterator();
        while(itFiles.hasNext()){
            DFile f = itFiles.next();
            if(f.getName().equals(file)){
                return true;
            }
        }
        return false;
    }

    public boolean containsFolder(String file){
        Iterator<Folder> itFolders = folders.iterator();
        while(itFolders.hasNext()){
            Folder f = itFolders.next();
            if(f.getName().equals(file)){
                return true;
            }
        }
        return false;
    }

    public Folder getRoot() {
        return root;
    }
}
