package com.jeklsoft.hector;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;

public class EmbeddedCassandra {
    private String cassandraConfigDirPath;
    private List<String> cassandraStartupCommands;

    private boolean cleanCassandra = false;
    private String hostname = "localhost";
    private int hostport = 9160;

	public void init() throws IOException {

        setupStorageConfigPath();

        if (cleanCassandra)
        {
            clean();
        }

		EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
		cassandra.start();

        if (cassandraStartupCommands != null)
        {
            executeCommands();
        }
	}

	protected void setupStorageConfigPath() throws IOException {
        if (cassandraConfigDirPath != null)
        {
            File configFile = new File(cassandraConfigDirPath);
            System.setProperty("cassandra.config", "file:" + configFile.getPath() + "/cassandra.yaml");
        }
        else
        {
            throw new IOException("Neither cassandraConfigDirResource nor cassandraConfigDirPath is configured, bailing.");
        }
	}

    public void setCassandraConfigDirPath(String cassandraConfigDirPath)
    {
        this.cassandraConfigDirPath = cassandraConfigDirPath;
    }

    public void setHostname(String hostname)
    {
        this.hostname = hostname;
    }

    public void setHostport(int hostport)
    {
        this.hostport = hostport;
    }

    public void setCassandraStartupCommands(List<String> cassandraCommands)
    {
        cassandraStartupCommands = cassandraCommands;
    }

    public void setCleanCassandra(Boolean cleanCassandra)
    {
        this.cleanCassandra = cleanCassandra;
    }

    private void executeCommands()
    {
        CliMain.connect(hostname, hostport);

        for (String command : cassandraStartupCommands)
        {
          CliMain.processStatement(command);
        }

        CliMain.disconnect();
    }

    private void clean() throws IOException {
      cleanupDataDirectories();
      makeDirsIfNotExist();
    }

    private void cleanupDataDirectories() throws IOException {
      for (String s : getDataDirs()) {
        cleanDir(s);
      }
    }

    private void makeDirsIfNotExist() throws IOException {
      for (String s : getDataDirs()) {
        mkdir(s);
      }
    }

    private Set<String> getDataDirs() {
      Set<String> dirs = new HashSet<String>();
      for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
        dirs.add(s);
      }
      dirs.add(DatabaseDescriptor.getCommitLogLocation());
      return dirs;
    }

    private void mkdir(String dir) throws IOException {
      FileUtils.createDirectory(dir);
    }

    private void cleanDir(String dir) throws IOException {
      File dirFile = new File(dir);
      deleteDir(dirFile);
    }

  private void deleteDir(File dir) {
      if (dir.isDirectory()) {
          String[] children = dir.list();
          for (int i=0; i<children.length; i++) {
              deleteDir(new File(dir, children[i]));
          }
      }

      dir.delete();
  }
}
