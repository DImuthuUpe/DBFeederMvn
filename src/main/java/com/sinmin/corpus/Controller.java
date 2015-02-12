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

package com.sinmin.corpus;

import com.sinmin.corpus.bean.DFile;
import com.sinmin.corpus.bean.Folder;
import com.sinmin.corpus.oracle.PLSQLClient;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by dimuthuupeksha on 11/28/14.
 */
public class Controller {
    final static Logger logger = Logger.getLogger(PLSQLClient.class);
    private Folder fileTree;
    private PLSQLClient oracleClient = new PLSQLClient();

    public Folder getFileTree() {
        return fileTree;
    }

    public void setFileTree(Folder fileTree) {
        this.fileTree = fileTree;
    }

    public void serializeFileTree(){
        try
        {
            Folder tmp = fileTree;
            while (tmp.getRoot()!=null){
                tmp = tmp.getRoot();
            }
            FileOutputStream fileOut = new FileOutputStream("filetree.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tmp);
            out.close();
            fileOut.close();
            System.out.printf("Serialized data is saved in filetree.ser");
        }catch(IOException i)
        {
            i.printStackTrace();
        }
    }

    public Folder deserializeFileTree(){
        Folder e = null;
        try
        {
            FileInputStream fileIn = new FileInputStream("filetree.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            e = (Folder) in.readObject();
            in.close();
            fileIn.close();
        }catch(IOException i)
        {
            logger.error(i);
            return null;
        }catch(ClassNotFoundException c)
        {
            logger.error(c);
            return null;
        }
        return e;
    }

    public void recursiveTravel(String currPath){
        File root =  new File(currPath);
        File [] listOfFiles = root.listFiles();
        for (int i=0;i<listOfFiles.length;i++){
            File file = listOfFiles[i];
            if(file.isFile()){
                if(!fileTree.containsFile(file.getName())){
                    logger.info("New file "+file);
                    DFile dfile = new DFile(file.getName(),fileTree);
                    fileTree.addFile(dfile);
                    if(file.getName().contains("xml")){
                        oracleClient.feed(file.toString());
                        serializeFileTree();
                    }
                }else{

                }
            }else{
                if(!fileTree.containsFolder(file.getName())){
                    logger.info("New folder " + file);
                    Folder folder = new Folder(file.getName(),fileTree);
                    fileTree.addFolder(folder);
                    fileTree = fileTree.getFolder(file.getName());
                    recursiveTravel(currPath+"/"+file.getName());
                    fileTree = fileTree.getRoot();
                }else{
                    fileTree = fileTree.getFolder(file.getName());
                    recursiveTravel(currPath+"/"+file.getName());
                    fileTree = fileTree.getRoot();
                }
            }
        }
    }

    public void Scanner(String path){

        while (true){

            recursiveTravel(path);
            try{
                Thread.sleep(1000);
            }catch (Exception e){

            }
        }



    }

    public void startFeeding(){
        fileTree = deserializeFileTree();
        if(fileTree==null){
            fileTree= new Folder("root",null);
        }
        //Scanner("/home/maduranga/crawler/data");
        Scanner(ConfigManager.getProperty(ConfigManager.DATA_ROOT));
    }

    public Controller(){
    }

    public static void main(String args[]){
        Controller con = new Controller();
        con.startFeeding();
    }

}
