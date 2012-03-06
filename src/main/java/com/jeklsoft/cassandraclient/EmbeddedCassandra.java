package com.jeklsoft.cassandraclient;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;

public class EmbeddedCassandra {
    private String cassandraConfigDirPath = "/tmp";
    private List<String> cassandraStartupCommands = new ArrayList<String>();

    private boolean cleanCassandra = false;
    private String hostname = "localhost";
    private int hostport = 9160;

    public static EmbeddedCassandraBuilder builder() {
        return new EmbeddedCassandraBuilder();
    }

    public static class EmbeddedCassandraBuilder {
        private final EmbeddedCassandra instance = new EmbeddedCassandra();

        public EmbeddedCassandraBuilder withCleanDataStore() {
            instance.setCleanCassandra(true);
            return this;
        }

        public EmbeddedCassandraBuilder withStartupCommands(List<String> cassandraCommands) {
            instance.setCassandraStartupCommands(cassandraCommands);
            return this;
        }

        public EmbeddedCassandraBuilder withHostname(String hostname) {
            instance.setHostname(hostname);
            return this;
        }

        public EmbeddedCassandraBuilder withHostport(int port) {
            instance.setHostport(port);
            return this;
        }

        public EmbeddedCassandraBuilder withCassandaConfigurationDirectoryPath(String path) {
            instance.setCassandraConfigDirPath(path);
            return this;
        }

        public EmbeddedCassandra build() {
            try {
                instance.init();
                return instance;
            }
            catch (IOException e) {
                throw new RuntimeException("Error building embedded Cassandra instance", e);
            }
        }
    }

    private EmbeddedCassandra() {
    }

    private void init() throws IOException {

        setupStorageConfigPath();

        if (cleanCassandra) {
            clean();
        }

        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();

        if (cassandraStartupCommands != null) {
            executeCommands();
        }
    }

    private void setupStorageConfigPath() throws IOException {
        if (cassandraConfigDirPath != null) {
            File configFile = new File(cassandraConfigDirPath);
            String configFileName = "file:" + configFile.getPath() + "/cassandra.yaml";
            System.setProperty("cassandra.config", configFileName);
        }
        else {
            throw new IOException("Neither cassandraConfigDirResource nor cassandraConfigDirPath is configured, bailing.");
        }
    }

    private void setCassandraConfigDirPath(String cassandraConfigDirPath) {
        this.cassandraConfigDirPath = cassandraConfigDirPath;
    }

    private void setHostname(String hostname) {
        this.hostname = hostname;
    }

    private void setHostport(int hostport) {
        this.hostport = hostport;
    }

    private void setCassandraStartupCommands(List<String> cassandraCommands) {
        cassandraStartupCommands = cassandraCommands;
    }

    private void setCleanCassandra(Boolean cleanCassandra) {
        this.cleanCassandra = cleanCassandra;
    }

    private void executeCommands() {
        CliMain.connect(hostname, hostport);

        for (String command : cassandraStartupCommands) {
            try {
                CliMain.processStatement(command);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
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
            for (int i = 0; i < children.length; i++) {
                deleteDir(new File(dir, children[i]));
            }
        }

        dir.delete();
    }
}
