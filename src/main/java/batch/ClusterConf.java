package batch;

public class ClusterConf {
    protected final String sparkIP;
    protected final String sparkPort;
    protected final String hdfsIP;
    protected final String hdfsPort;

    public String getSparkIP() {
        return sparkIP;
    }
    public String getSparkPort() {
        return sparkPort;
    }

    public String getHdfsIP() {
        return hdfsIP;
    }

    public String getHdfsPort() {
        return hdfsPort;
    }

    public ClusterConf(String hdfsIP, String hdfsPort, String sparkIP, String sparkPort) {
        this.sparkIP = sparkIP;
        this.sparkPort = sparkPort;
        this.hdfsIP = hdfsIP;
        this.hdfsPort = hdfsPort;
    }
}
