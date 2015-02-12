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
package com.sinmin.corpus.bean;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

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
