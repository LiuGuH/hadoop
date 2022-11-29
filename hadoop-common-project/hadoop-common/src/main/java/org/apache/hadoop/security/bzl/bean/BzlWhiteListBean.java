package org.apache.hadoop.security.bzl.bean;

import java.util.List;

public class BzlWhiteListBean {
  Meta meta;
  List<String> data;
  String pagination;

  public Meta getMeta() {
    return meta;
  }

  public void setMeta(Meta meta) {
    this.meta = meta;
  }

  public List<String> getData() {
    return data;
  }

  public void setData(List<String> data) {
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
}