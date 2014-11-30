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

    public Folder deserializeFileree(){
        Folder e = null;
        try
        {
            FileInputStream fileIn = new FileInputStream("/Users/dimuthuupeksha/filetree.ser");
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
            //c.printStackTrace();
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

    public Controller(){
        fileTree = deserializeFileree();
        if(fileTree==null){
            fileTree= new Folder("root",null);
        }
        Scanner("/home/maduranga/crawler/data");
        //Scanner("/Users/dimuthuupeksha/Documents/Academic/FYP/temp/out");


    }

    public static void main(String args[]){
        Controller con = new Controller();

    }

}
