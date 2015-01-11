import com.sinmin.corpus.Controller;
import com.sinmin.corpus.bean.Folder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by dimuthuupeksha on 1/9/15.
 */
public class TestDeserialization {
    @Before
    public void before(){
        File file  = new File("filetree.ser");
        file.delete();
    }

    @Test
    public void testDeserialization() throws URISyntaxException, IOException {
        URL url = this.getClass().getResource("filetree.ser");
        String workingDir = System.getProperty("user.dir");
        Path src = Paths.get(url.toURI());
        Path target = Paths.get(workingDir+File.separator+"filetree.ser");
        Files.copy(src,target, StandardCopyOption.REPLACE_EXISTING);

        Controller controller = new Controller();
        Folder folder = controller.deserializeFileree();

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
