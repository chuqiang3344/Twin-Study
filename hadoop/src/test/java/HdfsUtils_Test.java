import com.tyaer.hadoop.hdfs.HdfsUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Twin on 2017/8/9.
 */
public class HdfsUtils_Test {
//    public static String uri = "hdfs://192.168.1.118:9000";
    public static String uri = "";
    public String dir = "/user/output1";
    public String parentDir = "/user";

    @Test
    public void testMkdirNull1() {
        try{
            assertEquals(false, HdfsUtils.mkdir(null));
            assertEquals(false, HdfsUtils.mkdir(" "));
            assertEquals(false, HdfsUtils.mkdir(""));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testMkdirNormal1() {
        try{
            HdfsUtils.deleteDir(dir);
            boolean result = HdfsUtils.mkdir(dir);
            assertEquals(true, result);

            List<String> listFile = HdfsUtils.listAll(parentDir);
            boolean existFile = false;
            for(String elem : listFile){
                if(elem.equals(uri + dir)){
                    existFile = true;
                    break;
                }
            }
            assertEquals(true, existFile);
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testDeleteDirNull1() {
        try{
            assertEquals(false, HdfsUtils.deleteDir(null));
            assertEquals(false, HdfsUtils.deleteDir(""));
            assertEquals(false, HdfsUtils.deleteDir(" "));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testDeleteDir() {
        try{
            assertEquals(true, HdfsUtils.mkdir(dir));
            assertEquals(true, HdfsUtils.deleteDir(dir));
            List<String> listFile = HdfsUtils.listAll(parentDir);
            boolean existFile = false;
            for(String elem : listFile){
                if(uri + dir == elem){
                    existFile = true;
                    break;
                }
            }
            assertEquals(false, existFile);
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testListAllNull1() {
        try{
            List<String> listFile = new ArrayList<String>();
            assertEquals(listFile.toString(), HdfsUtils.listAll(null).toString());
            assertEquals(listFile.toString(), HdfsUtils.listAll(" ").toString());
            assertEquals(listFile.toString(), HdfsUtils.listAll("").toString());
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testListAllEmptyFolder() {
        try{
            HdfsUtils.deleteDir(dir);
            assertEquals(true, HdfsUtils.mkdir(dir));
            List<String> listFile = HdfsUtils.listAll(dir);
            assertEquals(0, listFile.size());
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testListAllNotExistFolder() {
        try{
            HdfsUtils.deleteDir(dir);
            List<String> listFile = HdfsUtils.listAll(dir);
            assertEquals(0, listFile.size());
        } catch(Exception ex){
            assertEquals(true, true);
        }
    }

    @Test
    public void testUploadLocalFile2HDFSNull1() {
        try{
            assertEquals(false, HdfsUtils.uploadLocalFile2HDFS(null, null));
            assertEquals(false, HdfsUtils.uploadLocalFile2HDFS("", ""));
            assertEquals(false, HdfsUtils.uploadLocalFile2HDFS(" ", " "));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testUploadLocalFile2HDFS() {
        String localFile = "F:/Program Files/eclipse/eclipse.ini";
        String remoteFile = dir + "/eclipse.ini";

        try{
            HdfsUtils.mkdir(dir);
            HdfsUtils.deleteHDFSFile(remoteFile);
            assertEquals(true, HdfsUtils.uploadLocalFile2HDFS(localFile, remoteFile));
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testUploadLocalFile2HDFSLocalNotExist() {
        String localFile = "F:/Program Files/eclipse/eclipse2.ini";
        String remoteFile = dir + "/eclipse.ini";

        try{
            assertEquals(true, HdfsUtils.mkdir(dir));
            HdfsUtils.deleteHDFSFile(remoteFile);
            HdfsUtils.uploadLocalFile2HDFS(localFile, remoteFile);
        } catch(Exception ex){
            assertEquals(true, true);
        }
    }

    @Test
    public void testCreateNewHDFSFileNull1() {
        try{
            assertEquals(false, HdfsUtils.createNewHDFSFile(null, null));
            assertEquals(false, HdfsUtils.createNewHDFSFile(" ", " "));
            assertEquals(false, HdfsUtils.createNewHDFSFile("", ""));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testCreateNewHDFSFileNormal1() {
        try{
            String newFile = dir + "/file1.txt";
            String content = "hello file1";

            HdfsUtils.deleteHDFSFile(newFile);
            assertEquals(true, HdfsUtils.createNewHDFSFile(newFile, content));
            String result = new String(HdfsUtils.readHDFSFile(newFile));
            assertEquals(content, result);
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testCreateNewHDFSFileFoldNotexist1() {
        try{
            String newFile = dir + "/file1.txt";
            String content = "hello file1";

            assertEquals(true, HdfsUtils.deleteDir(dir));
            assertEquals(true, HdfsUtils.createNewHDFSFile(newFile, content));
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testDeleteHDFSFileNull1() {
        try{
            assertEquals(false, HdfsUtils.deleteHDFSFile(null));
            assertEquals(false, HdfsUtils.deleteHDFSFile(" "));
            assertEquals(false, HdfsUtils.deleteHDFSFile(""));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testDeleteHDFSFile() {
        this.testUploadLocalFile2HDFS();
        try{
            String remoteFile = dir + "/eclipse.ini";
            assertEquals(true, HdfsUtils.deleteHDFSFile(remoteFile));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testDeleteHDFSFileNotexist1() {
        try{
            String remoteFile = dir + "/eclipse2.ini";
            assertEquals(false, HdfsUtils.deleteHDFSFile(remoteFile));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testReadHDFSFileNull1() {
        try{
            assertEquals(null, HdfsUtils.readHDFSFile(null));
            assertEquals(null, HdfsUtils.readHDFSFile(" "));
            assertEquals(null, HdfsUtils.readHDFSFile(""));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testReadHDFSFile() {
        this.testUploadLocalFile2HDFS();
        try{
            String remoteFile = dir + "/eclipse.ini";
            String result = new String(HdfsUtils.readHDFSFile(remoteFile));
            assertEquals(true, result.length() > 0);
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }

    @Test
    public void testAppendNull1() {
        try{
            assertEquals(false, HdfsUtils.append(null, null));
            assertEquals(false, HdfsUtils.append(" ", " "));
            assertEquals(false, HdfsUtils.append("", ""));
        } catch(Exception ex){
            assertEquals(true, false);
        }
    }

    @Test
    public void testAppend() {
        try{
            String newFile = dir + "/file1.txt";
            String content1 = "hello append1\r\n";
            String content2 = "hello append2\r\n";

            HdfsUtils.deleteHDFSFile(newFile);
            assertEquals(true, HdfsUtils.createNewHDFSFile(newFile, ""));
            assertEquals(true, HdfsUtils.append(newFile, content1));
            assertEquals(content1, new String(HdfsUtils.readHDFSFile(newFile)));
            assertEquals(true, HdfsUtils.append(newFile, content2));
            assertEquals(content1 + content2, new String(HdfsUtils.readHDFSFile(newFile)));
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }
}
