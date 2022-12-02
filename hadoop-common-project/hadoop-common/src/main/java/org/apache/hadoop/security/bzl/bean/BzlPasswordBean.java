package org.apache.hadoop.security.bzl.bean;

import java.util.List;

public class BzlPasswordBean {
  Meta meta;
  List<PasswordData> data;
  String pagination;

  public Meta getMeta() {
    return meta;
  }

  public void setMeta(Meta meta) {
    this.meta = meta;
  }

  public List<PasswordData> getData() {
    return data;
  }

  public void setData(List<PasswordData> data) {
    this.data = data;
  }

  public String getPagination() {
    return pagination;
  }

  public void setPagination(String pagination) {
    this.pagination = pagination;
  }

  public static class Meta {
    int code;
    String errorMsg;
    String traceId;
    String host;

    public int getCode() {
      return code;
    }

    public void setCode(int code) {
      this.code = code;
    }

    public String getErrorMsg() {
      return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
      this.errorMsg = errorMsg;
    }

    public String getTraceId() {
      return traceId;
    }

    public void setTraceId(String traceId) {
      this.traceId = traceId;
    }

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }
  }

  public static class PasswordData {
    String groupAccount;
    String groupPassword;
    String expiringGroupPassword;

    public String getGroupAccount() {
      return groupAccount;
    }

    public void setGroupAccount(String groupAccount) {
      this.groupAccount = groupAccount;
    }

    public String getGroupPassword() {
      return groupPassword;
    }

    public void setGroupPassword(String groupPassword) {
      this.groupPassword = groupPassword;
    }

    public String getExpiringGroupPassword() {
      return expiringGroupPassword;
    }

    public void setExpiringGroupPassword(String expiringGroupPassword) {
      this.expiringGroupPassword = expiringGroupPassword;
    }
  }
}