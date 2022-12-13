package org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics.bean;

/**
 * correspond to LiveNodes entity
 */
public class LiveDataNodeBean {

  private String hostname;
  private String hostnamePlusPort;
  private String infoAddr;
  private String infoSecureAddr;
  private String xferaddr;
  private String version;
  private long lastContact;
  private long usedSpace;
  private long nonDfsUsedSpace;
  private long capacity;
  private int numBlocks;
  private long used;
  private long remaining;
  private long blockPoolUsed;
  private long lastBlockReport;
  private float blockPoolUsedPercent;
  private int blockScheduled;
  private int adminState;
  private int volfails;


  public LiveDataNodeBean() {
  }

  public LiveDataNodeBean(String hostname, int lastContact) {
    this.hostname = hostname;
    this.lastContact = lastContact;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getHostnamePlusPort() {
    return hostnamePlusPort;
  }

  public void setHostnamePlusPort(String hostnamePlusPort) {
    this.hostnamePlusPort = hostnamePlusPort;
  }

  public String getInfoAddr() {
    return infoAddr;
  }

  public void setInfoAddr(String infoAddr) {
    this.infoAddr = infoAddr;
  }

  public String getInfoSecureAddr() {
    return infoSecureAddr;
  }

  public void setInfoSecureAddr(String infoSecureAddr) {
    this.infoSecureAddr = infoSecureAddr;
  }

  public String getXferaddr() {
    return xferaddr;
  }

  public void setXferaddr(String xferaddr) {
    this.xferaddr = xferaddr;
  }

  public long getLastContact() {
    return lastContact;
  }

  public void setLastContact(long lastContact) {
    this.lastContact = lastContact;
  }

  public long getUsedSpace() {
    return usedSpace;
  }

  public void setUsedSpace(long usedSpace) {
    this.usedSpace = usedSpace;
  }

  public int getAdminState() {
    return adminState;
  }

  public void setAdminState(int adminState) {
    this.adminState = adminState;
  }

  public long getNonDfsUsedSpace() {
    return nonDfsUsedSpace;
  }

  public void setNonDfsUsedSpace(long nonDfsUsedSpace) {
    this.nonDfsUsedSpace = nonDfsUsedSpace;
  }

  public long getCapacity() {
    return capacity;
  }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public int getNumBlocks() {
    return numBlocks;
  }

  public void setNumBlocks(int numBlocks) {
    this.numBlocks = numBlocks;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public long getUsed() {
    return used;
  }

  public void setUsed(long used) {
    this.used = used;
  }

  public long getRemaining() {
    return remaining;
  }

  public void setRemaining(long remaining) {
    this.remaining = remaining;
  }

  public int getBlockScheduled() {
    return blockScheduled;
  }

  public void setBlockScheduled(int blockScheduled) {
    this.blockScheduled = blockScheduled;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  public void setBlockPoolUsed(long blockPoolUsed) {
    this.blockPoolUsed = blockPoolUsed;
  }

  public float getBlockPoolUsedPercent() {
    return blockPoolUsedPercent;
  }

  public void setBlockPoolUsedPercent(float blockPoolUsedPercent) {
    this.blockPoolUsedPercent = blockPoolUsedPercent;
  }

  public int getVolfails() {
    return volfails;
  }

  public void setVolfails(int volfails) {
    this.volfails = volfails;
  }

  public long getLastBlockReport() {
    return lastBlockReport;
  }

  public void setLastBlockReport(long lastBlockReport) {
    this.lastBlockReport = lastBlockReport;
  }
}
