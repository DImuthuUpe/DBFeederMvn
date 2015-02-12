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

import com.sinmin.corpus.Controller;
import com.sinmin.corpus.bean.DFile;
import com.sinmin.corpus.bean.Folder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TestSerialization {

    @Before
    public void before(){
        File file  = new File("filetree.ser");
        file.delete();
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        Folder rootfolder = new Folder("root",null);
        DFile file1 = new DFile("file1",rootfolder);
        rootfolder.addFile(file1);

        Folder leve11_1 = new Folder("level1_1",rootfolder);
        rootfolder.addFolder(leve11_1);

        Folder leve11_2 = new Folder("level1_2",rootfolder);
        rootfolder.addFolder(leve11_2);

        Folder leve12_1 = new Folder("level2_1",leve11_1);
        leve11_1.addFolder(leve12_1);

        Folder leve12_2 = new Folder("level2_2",leve11_1);
        leve11_1.addFolder(leve12_2);


        DFile file2 = new DFile("file2",leve12_2);
        leve12_2.addFile(file2);

        Controller controller = new Controller();
        controller.setFileTree(rootfolder);
        controller.serializeFileTree();

        FileInputStream fileIn = new FileInputStream("filetree.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Folder folder = (Folder) in.readObject();
        in.close();
        fileIn.close();


        assertEquals(folder.getRoot(), null);
        assertEquals(folder.containsFolder("level1_1"),true);
        assertEquals(folder.containsFolder("level1_2"), true);
        assertEquals(folder.getFolder("level1_1").containsFolder("level2_1"),true);
        assertEquals(folder.getFolder("level1_1").containsFolder("level2_2"),true);
        assertEquals(folder.containsFile("file1"),true);
        assertEquals(folder.getFolder("level1_1").getFolder("level2_2").containsFile("file2"),true);

    }

    @After
    public void after(){
        File file  = new File("filetree.ser");
        file.delete();
    }

}
