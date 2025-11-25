package org.apache.hadoop.security.bzl.auth;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlAuthFileUtils;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;

import static org.apache.hadoop.security.bzl.auth.BzlTokenPasswordUpdateThread.GROUP_PASSWORD_FILE_PREFIX;
import static org.apache.hadoop.security.bzl.auth.BzlTokenPasswordUpdateThread.SPECIFIED_GROUP_PASSWORD;
import static org.apache.hadoop.security.bzl.auth.BzlTokenPasswordUpdateThread.getCurrentDataString;
import static org.junit.Assert.assertEquals;

public class TestBzlAuthPasswordFile {
  String bzlAuthLocalDir = "/Users/admin/bzlauth";
  String bzlAuthUrl = "http://alps-auth-web-datastar-qa.kanzhun.tech/api/alps/auth/groupUser/query";
  String bzlAuthUrlAc = "";
  String bzlAuthUrlSk = "";

  String bzlAuthFilePathPrefix = bzlAuthLocalDir + File.separator + GROUP_PASSWORD_FILE_PREFIX;

  @Test
  public void testFindLatestGroupPasswordFileAndRetainMaxVersions() throws Exception {

    RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics =
        RpcBzlTokenPasswordFetcherMetrics.create();
    for (int i = 0; i < 5; i++) {
      List<BzlPasswordBean.PasswordData> passwordList =
          BzlJsonUtils.getGroupPasswordList(bzlAuthUrl, bzlAuthUrlAc, bzlAuthUrlSk,
              rpcBzlTokenPasswordFetcherMetrics, "[BDH]groupQuery");

      BzlAuthFileUtils.writePasswordFile(passwordList,
          bzlAuthFilePathPrefix + getCurrentDataString(), rpcBzlTokenPasswordFetcherMetrics);

      Thread.sleep(1000);
    }

    List<Path> paths = BzlAuthFileUtils.findLatestGroupPasswordFileAndRetainMaxVersions(bzlAuthLocalDir,
        SPECIFIED_GROUP_PASSWORD, GROUP_PASSWORD_FILE_PREFIX, 3);

    //assertEquals(paths.size(), 3);
  }

  @Test
  public void testBzlTokenPasswordUpdateThread() {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_BZL_AUTH_LOCALDIR, bzlAuthLocalDir);
    conf.set(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL,
        "http://alps-auth-web-datastar-qa.kanzhun.tech/api/alps/auth/groupUser/query");
    conf.set(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_AC, bzlAuthUrlAc);
    conf.set(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_SK, bzlAuthUrlSk);

    BzlDynamicConfiguration.getInstance().updateConfiguration(conf);


    BzlTokenPasswordUpdateThread thread = new BzlTokenPasswordUpdateThread(conf);
    thread.run();
  }

}
