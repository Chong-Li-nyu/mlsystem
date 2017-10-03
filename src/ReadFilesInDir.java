import java.io.File;
import java.util.*;



public class ReadFilesInDir{
    
    public List<File> listFilesForFolder(final File folder) {
        List<File> datafiles = new ArrayList<File>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                //System.out.println(fileEntry.getName());
                datafiles.add(fileEntry);
            }
        }
        return datafiles;
        
    }
    public List<File> listDataFiles(){
    	System.out.println(System.getProperty("user.dir"));
    
    	File workingDir = new File(System.getProperty("user.dir"));
    	File parentDirRelative= workingDir.getParentFile();
    	File dataDir =  new File(parentDirRelative+"/data");
    	//File parentDir =  new File(parentDirRelative.getAbsolutePath()+"/data");
    
    	return listFilesForFolder(dataDir);
    }

}
