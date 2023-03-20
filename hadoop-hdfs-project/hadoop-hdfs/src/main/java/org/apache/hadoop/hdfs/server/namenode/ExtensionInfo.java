package org.apache.hadoop.hdfs.server.namenode;

/**
 * As a extension properties used in audit log.
 * It could contain some informations such as the affected block number of rpc..
 * the parttern as below:
 * extensionInfo=numBlocks:1250,xxx:yyy
 */
public class ExtensionInfo {
  private int numBlocks;

  public ExtensionInfo() {
  }

  public ExtensionInfo(int numBlocks) {
    this.numBlocks = numBlocks;
  }

  public int getNumBlocks() {
    return numBlocks;
  }

  public void setNumBlocks(int numBlocks) {
    this.numBlocks = numBlocks;
  }
}
