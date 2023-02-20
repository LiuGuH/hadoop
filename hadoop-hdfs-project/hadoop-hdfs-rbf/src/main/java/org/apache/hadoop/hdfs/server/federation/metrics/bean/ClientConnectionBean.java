package org.apache.hadoop.hdfs.server.federation.metrics.bean;

public class ClientConnectionBean {

  private String user;
  private String nnId;
  private int active;
  private int recent_active;
  private int idle;
  private int total;

  public ClientConnectionBean() {
  }

  public ClientConnectionBean(String user, String nnId, int active, int recent_active, int idle, int total) {
    this.user = user;
    this.nnId = nnId;
    this.active = active;
    this.recent_active = recent_active;
    this.idle = idle;
    this.total = total;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getNnId() {
    return nnId;
  }

  public void setNnId(String nnId) {
    this.nnId = nnId;
  }

  public int getActive() {
    return active;
  }

  public void setActive(int active) {
    this.active = active;
  }

  public int getRecent_active() {
    return recent_active;
  }

  public void setRecent_active(int recent_active) {
    this.recent_active = recent_active;
  }

  public int getIdle() {
    return idle;
  }

  public void setIdle(int idle) {
    this.idle = idle;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }
}
