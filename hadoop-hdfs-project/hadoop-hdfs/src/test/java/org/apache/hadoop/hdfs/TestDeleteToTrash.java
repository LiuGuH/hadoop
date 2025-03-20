package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.BzlForceToTrashDirectoriesUpdater;
import org.apache.hadoop.hdfs.server.namenode.DeleteToTrashUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE;
import static org.apache.hadoop.hdfs.server.namenode.DeleteToTrashUtils.CURRENT;
import static org.apache.hadoop.hdfs.server.namenode.DeleteToTrashUtils.NN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestDeleteToTrash {
  static final Logger LOG = LoggerFactory.getLogger(TestDeleteToTrash.class);


  @Test
  public void testRenameDir() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build()) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/dir0/sub01/sub02"));
      dfs.mkdirs(new Path("/dir0/sub01/sub02/sub03"));
      dfs.mkdirs(new Path("/dir0/sub01/sub02/sub04"));

      String[] files = new String[] {"/dir0/sub01/sub02/file1", "/dir0/sub01/sub02/sub03/file2",
          "/dir0/sub01/sub02/sub04/file3"};
      for (int i = 0; i < files.length; i++) {
        FSDataOutputStream output = dfs.create(new Path(files[i]));
        output.writeBytes("Some test data to write longer than 10 bytes");
        output.close();
      }

      // dst.getParent does not exist, rename failed.
      dfs.mkdirs(new Path("/dir1/"));
      boolean success = dfs.rename(new Path("/dir0/sub01/sub02"), new Path("/dir1/sub11/sub12"));
      assertFalse(success);

      // dst.getParent exists, rename success.
      dfs.mkdirs(new Path("/dir1/sub11"));
      boolean success1 = dfs.rename(new Path("/dir0/sub01/sub02"), new Path("/dir1/sub11/sub12"));
      assertTrue(success1);
      assertTrue(dfs.exists(new Path("/dir1/sub11/sub12/file1")));
      assertTrue(dfs.exists(new Path("/dir1/sub11/sub12/sub03")));
      assertTrue(dfs.exists(new Path("/dir1/sub11/sub12/sub04")));

      // reset
      dfs.rename(new Path("/dir1/sub11/sub12"), new Path("/dir0/sub01/sub02"));
      assertTrue(dfs.exists(new Path("/dir0/sub01/sub02/file1")));
      assertTrue(dfs.exists(new Path("/dir0/sub01/sub02/sub03/file2")));
      assertTrue(dfs.exists(new Path("/dir0/sub01/sub02/sub04/file3")));

      // dst exists and /dir1/sub11/sub12/sub02 does not exist
      dfs.mkdirs(new Path("/dir1/sub11/sub12"));
      boolean success2 = dfs.rename(new Path("/dir0/sub01/sub02"), new Path("/dir1/sub11/sub12"));
      assertTrue(success2);
      assertTrue(dfs.exists(new Path("/dir1/sub11/sub12/sub02/file1")));
      assertTrue(dfs.exists(new Path("/dir1/sub11/sub12/sub02/sub03/file2")));
      assertTrue(dfs.exists(new Path("/dir1/sub11/sub12/sub02/sub04/file3")));

      // reset
      dfs.rename(new Path("/dir1/sub11/sub12/sub02"), new Path("/dir0/sub01"));
      assertTrue(dfs.exists(new Path("/dir0/sub01/sub02/file1")));
      assertTrue(dfs.exists(new Path("/dir0/sub01/sub02/sub03/file2")));
      assertTrue(dfs.exists(new Path("/dir0/sub01/sub02/sub04/file3")));

      // dst exists and /dir1/sub11/sub12/sub02  exists
      dfs.mkdirs(new Path("/dir1/sub11/sub12/sub02"));
      boolean success3 = dfs.rename(new Path("/dir0/sub01/sub02"), new Path("/dir1/sub11/sub12"));
      assertFalse(success3);
    }
  }

  @Test
  public void testRenameFile() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build()) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/dir0/sub01"));

      String[] files = new String[] {"/dir0/sub01/file1"};
      for (int i = 0; i < files.length; i++) {
        FSDataOutputStream output = dfs.create(new Path(files[i]));
        output.writeBytes("Some test data to write longer than 10 bytes");
        output.close();
      }

      // dst.getParent does not exist, rename failed.
      dfs.mkdirs(new Path("/dir1"));
      boolean success = dfs.rename(new Path("/dir0/sub01/file1"), new Path("/dir1/sub11/file2"));
      assertFalse(success);

      // dst.getParent exists, rename success.
      dfs.mkdirs(new Path("/dir1/sub11"));
      boolean success1 = dfs.rename(new Path("/dir0/sub01/file1"), new Path("/dir1/sub11/file2"));
      assertTrue(success1);
      assertFalse(dfs.exists(new Path("/dir0/sub01/file1")));
      assertTrue(dfs.exists(new Path("/dir1/sub11/file2")));

      // reset
      dfs.rename(new Path("/dir1/sub11/file2"), new Path("/dir0/sub01/file1"));
      assertTrue(dfs.exists(new Path("/dir0/sub01/file1")));
      assertFalse(dfs.exists(new Path("/dir1/sub11/file2")));

      // dst exists and is a file
      FSDataOutputStream output = dfs.create(new Path("/dir1/sub11/file2"));
      output.writeBytes("Some test data to write longer than 10 bytes");
      output.close();
      boolean success2 = dfs.rename(new Path("/dir0/sub01/file1"), new Path("/dir1/sub11/file2"));
      assertFalse(success2);
    }
  }

  @Test
  public void testRename2Options() throws Exception {
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build()) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      Path path = new Path("/test");
      dfs.mkdirs(path);
      GenericTestUtils.LogCapturer auditLog =
          GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.auditLog);
      dfs.rename(path, new Path("/dir1"), new Rename[] {Rename.OVERWRITE, Rename.TO_TRASH});
      String auditOut = auditLog.getOutput();
      assertTrue(
          "Rename should have both OVERWRITE and TO_TRASH " + "flags at namenode but had only "
              + auditOut, auditOut.contains("options=[OVERWRITE, TO_TRASH]"));
    }
  }

  @Test
  public void testMidirs() throws Exception {
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build()) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();

      Path path = new Path("/test/subdir1/subdir2/subdir3");
      boolean success = dfs.mkdirs(path);
      assertTrue(success);
      success = dfs.mkdirs(path);
      assertTrue(success);
      dfs.delete(new Path("/test"));

      // create a new file.
      Path file = new Path("/test/subdir1");
      FSDataOutputStream output = dfs.create(file);
      // write to file
      output.writeBytes("Some test data");
      output.flush();
      output.close();

      try {
        success = dfs.mkdirs(file);
      } catch (FileAlreadyExistsException e) {
        success = false;
      }
      assertFalse(success);
    }
  }

  @Test
  public void testTrashWhenInodeExist() throws Exception {
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build()) {
      cluster.waitActive();
      final DistributedFileSystem fs = cluster.getFileSystem();
      Configuration conf = fs.getConf();
      conf.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 120 * 1000);

      Path path = new Path("/test1/sub01/sub02");
      Path trashRoot = fs.getTrashRoot(path);
      Path trashCurrent = new Path(trashRoot, new Path("Current"));

      fs.mkdirs(path);
      Trash.moveToAppropriateTrash(fs, path, conf);
      assertTrue(fs.exists(Path.mergePaths(trashCurrent, path)));

      fs.mkdirs(path);
      Trash.moveToAppropriateTrash(fs, path, conf);
      assertTrue(fs.exists(Path.mergePaths(trashCurrent, new Path("/test1/sub01/sub02"))));

      fs.delete(trashCurrent, true);
      fs.delete(new Path("/test1"), true);

      path = new Path("/test1/sub01");
      fs.mkdirs(new Path("/test1"));

      FSDataOutputStream outputStream = fs.create(path);
      outputStream.writeBytes("This is a message.");
      outputStream.close();

      Trash.moveToAppropriateTrash(fs, path, conf);

      path = new Path("/test1/sub01/sub02");
      fs.mkdirs(path);
      Trash.moveToAppropriateTrash(fs, path, conf);
      assertEquals(2, fs.listStatus(Path.mergePaths(trashCurrent, new Path("/test1"))).length);
    }
  }

  @Test
  public void testDeleteToRename() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 120 * 1000);
    BzlDynamicConfiguration.getInstance().set(FS_FORCE_TO_TRASH_BZL_ENABLE, "true");

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();

      TreeSet<String> protectToTrashDirs = new TreeSet<String>();
      protectToTrashDirs.add("/test");
      cluster.getNamesystem().getFSDirectory().setForceToTrashDirectories(protectToTrashDirs);

      final DistributedFileSystem fs = cluster.getFileSystem();

      Path path = new Path("/test/sub01/sub02/sub03");
      fs.mkdirs(path);
      fs.delete(path, true);

      fs.mkdirs(path);
      fs.delete(path, true);

      Path trashRoot = Path.mergePaths(fs.getTrashRoot(path), new Path(CURRENT + NN));
      assertEquals(2,
          fs.listStatus(Path.mergePaths(trashRoot, new Path("/test/sub01/sub02"))).length);

      fs.delete(new Path("/test"));
      fs.delete(trashRoot, true);

      Path filePath = new Path("/test");
      FSDataOutputStream output = fs.create(filePath);
      output.writeBytes("This is a file.");
      output.close();

      fs.delete(filePath, true);

      fs.mkdirs(path);
      fs.delete(path, true);
      assertEquals(2, fs.listStatus(trashRoot).length);

      fs.delete(new Path("/test"), true);
      fs.delete(trashRoot, true);

      fs.mkdirs(new Path("/user/test"));
      fs.setOwner(new Path("/"),"test","test");
      fs.setOwner(new Path("/user"),"test","test");
      fs.setOwner(new Path("/user/test"),"test","test");

      UserGroupInformation testUser = UserGroupInformation.createRemoteUser("test");
      testUser.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException {
          FileSystem fs = FileSystem.get(cluster.getConfiguration(0));
          Path filePath = new Path("/test/sub01");
          FSDataOutputStream output = fs.create(filePath);
          output.writeBytes("This is a file.");
          output.close();
          fs.delete(filePath, true);

          Path path = new Path("/test/sub01/sub02/sub03");
          fs.mkdirs(path);

          fs.delete(path, true);
          Path trashRoot = Path.mergePaths(fs.getTrashRoot(path), new Path(CURRENT + NN + "/test"));
          assertEquals(2, fs.listStatus(trashRoot).length);
          return null;
        }
      });

    }
  }

  @Test
  public void testDeleteWhenSrcNotExists() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 120 * 1000);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      // Delete failed when path does not success and do not throw exception.
      boolean success = fs.delete(new Path("/a/b/c"), true);
      assertFalse(success);

      success = fs.delete(new Path("/a/b/c"), false);
      assertFalse(success);

      //
      fs.mkdirs(new Path("/a/b/c"));
      success = fs.delete(new Path("/a/b/c"), false);
      assertTrue(success);
    }
  }

  @Test
  public void testDeleteToRenameWhenRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 120 * 1000);
    BzlDynamicConfiguration.getInstance().set(FS_FORCE_TO_TRASH_BZL_ENABLE,"true");

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();

      TreeSet<String> protectToTrashDirs = new TreeSet<String>();
      protectToTrashDirs.add("/test");
      cluster.getNamesystem().getFSDirectory().setForceToTrashDirectories(protectToTrashDirs);

      DistributedFileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test/sub01/sub02/sub03");

      fs.mkdirs(path);
      path = path.getParent();
      // test delete with false when inode has children，it will throw IOException
      boolean isError = false;
      try {
        fs.delete(path, false);
      } catch (PathIsNotEmptyDirectoryException e) {
        isError = true;
      }
      assertTrue(isError);

      // test delete with true when inode has children，it will rename to Trash
      fs.delete(path, true);
      assertFalse(fs.exists(path));
      Path trashRoot = Path.mergePaths(fs.getTrashRoot(path), new Path(CURRENT + NN));
      assertTrue(fs.exists(Path.mergePaths(trashRoot, path)));
      fs.delete(trashRoot, true);

      fs.mkdirs(path);
      // test delete with false when inode has no children，it will rename to Trash
      fs.delete(path, false);
      assertTrue(fs.exists(Path.mergePaths(trashRoot, path)));

      // test delete when path does not exist, return success
      boolean success = fs.delete(path, false);
      assertFalse(success);

      success = fs.delete(path, true);
      assertFalse(success);
    }
  }

  @Test
  public void testGetPathParent() {
    String test = "/a";
    String result = DeleteToTrashUtils.getPathParent(test);
    System.out.println(result);

    System.out.println("/a".substring(0, 0));
  }

  @Test
  public void tesSrc() {
    assertTrue("/a/b/c".startsWith("/a/b/c"));
    assertFalse("/a/b".startsWith("/a/b/c"));

    assertTrue("a".substring(0, 0).isEmpty());

    String s = "/a/b/c/";
    assertEquals(7,s.length());
    assertEquals("/a/b/c/",s.substring(0,s.length()));
    assertEquals("/a/b/c",s.substring(0,s.length()-1));
  }

  @Test
  public void testSortedSet() {
    SortedSet<String> protectedDirs = Collections.synchronizedSortedSet(new TreeSet<>());
    protectedDirs.add("/a/b/c");
    protectedDirs.add("/a/d/d");
    protectedDirs.add("/a");

    String path = "/a";
    SortedSet<String> s = protectedDirs.subSet(path, path + "0");
  }

  @Test
  public void testRpcGetListing() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build()) {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      NamenodeProtocols namenodeProtocols = cluster.getNameNodeRpc();
      String src = "/a/b/c";
      // Test when src does not exist,  directoryListing is null
      DirectoryListing directoryListing =
          namenodeProtocols.getListing(src, HdfsFileStatus.EMPTY_NAME, false);
      assertNull(directoryListing);

      dfs.mkdirs(new Path(src));
      directoryListing = namenodeProtocols.getListing(src, HdfsFileStatus.EMPTY_NAME, false);
      assertNotNull(directoryListing);
      assertNotNull(directoryListing.getPartialListing());
      assertEquals(0, directoryListing.getPartialListing().length);

      dfs.mkdirs(new Path(src + "/d"));
      directoryListing = namenodeProtocols.getListing(src, HdfsFileStatus.EMPTY_NAME, false);
      assertNotNull(directoryListing);
      assertNotNull(directoryListing.getPartialListing());
      assertEquals(1, directoryListing.getPartialListing().length);
      assertFalse(directoryListing.hasMore());

      dfs.delete(new Path("/a"));

      src = "/a/b";
      FSDataOutputStream outputStream = dfs.create(new Path(src));
      outputStream.writeBytes("This is a file.");
      outputStream.close();
      directoryListing = namenodeProtocols.getListing(src, HdfsFileStatus.EMPTY_NAME, false);
      assertNotNull(directoryListing);
      assertNotNull(directoryListing.getPartialListing());
      assertEquals(1, directoryListing.getPartialListing().length);
      assertFalse(directoryListing.hasMore());
    }
  }

  @Test
  public void testRpcGetFileInfo() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build()) {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      NamenodeProtocols namenodeProtocols = cluster.getNameNodeRpc();
      String src = "/a/b/c";
      HdfsFileStatus hdfsFileStatus = namenodeProtocols.getFileInfo(src);
      assertNull(hdfsFileStatus);

      dfs.mkdirs(new Path(src));
      hdfsFileStatus = namenodeProtocols.getFileInfo(src);
      assertNotNull(hdfsFileStatus);
    }
  }

  @Test
  public void test() {
    String url = System.getProperty("url",
        "https://datastar-dev.weizhipin.com/api/guardian/manage/hdfs/deletetotrash/dir/find?clusterCode=dap-hadoop-dev");
    LOG.info("URL is {}", url);
    String jsonData = doGetHttp(url);
    LOG.info("Returned json data is: {}", jsonData);
    SortedSet<String> set = parseJson(jsonData);
    LOG.info("Set is: {}", set);
  }

  private String doGetHttp(String forceToTrashDirectoriesBzlRemoteUrl) {
    String resStr = null;
    CloseableHttpClient httpClient = null;
    CloseableHttpResponse httpResponse = null;
    try {
      URI uri = new URIBuilder(forceToTrashDirectoriesBzlRemoteUrl).build();
      httpClient = HttpClients.createDefault();
      HttpGet httpGet = new HttpGet(uri);
      httpResponse = httpClient.execute(httpGet);

      if (httpResponse.getStatusLine().getStatusCode() == 200) {
        resStr = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      } else {
        LOG.warn("Fetch error. The return code is {} .",
            httpResponse.getStatusLine().getStatusCode());
      }
    } catch (IOException e) {
      LOG.warn("IOException error! The detail message is {}.", e.getMessage());
      return null;
    } catch (URISyntaxException e) {
      LOG.warn("URISyntaxException error! The detail message is {}.", e.getMessage());
      return null;
    } finally {
      try {
        if (httpClient != null) {
          httpClient.close();
        }
        if (httpResponse != null) {
          httpResponse.close();
        }
      } catch (IOException e) {
        LOG.warn("Close error! The detail message is {}.", e.getMessage());
      }
    }
    return resStr;
  }

  private SortedSet<String> parseJson(String json) {
    SortedSet<String> remoteForceToTrashDirectories = new TreeSet<>();
    if (StringUtils.isEmpty(json)) {
      return remoteForceToTrashDirectories;
    }

    Gson gson = new Gson();
    Map<String, Map<String, List<String>>> rs = gson.fromJson(json, Map.class);
    Map<String, List<String>> allData = rs.get("data");
    int remoteForceToTrashDirectoriesNums = 0;

    if (allData == null) {
      LOG.warn("remoteForceToTrashDirectoriesNums is null");
    }

    List<String> remoteForceToTrashDirectoriesList = new ArrayList<>();
    for (Map.Entry<String, List<String>> item : allData.entrySet()) {
      List<String> itemList = item.getValue();
      remoteForceToTrashDirectoriesNums += itemList.size();
      remoteForceToTrashDirectoriesList.addAll(itemList);
    }

    if (!checkLegality(remoteForceToTrashDirectoriesList)) {
      LOG.warn("RemoteForceToTrashDirectories check legality failed. Will not update.");
      return remoteForceToTrashDirectories;
    }

    if (remoteForceToTrashDirectoriesList.size() > BzlDynamicConfiguration.getInstance()
        .getLong(FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE, 100000)) {
      LOG.warn("RemoteForceToTrashDirectoriesList size exceeds the upper limit of {}.",
          BzlDynamicConfiguration.getInstance()
              .getLong(FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE, 100000));
      return remoteForceToTrashDirectories;
    }

    // If path ends with /, like '/a/b/c/'. Make it to '/a/b/c'
    for (int i = 0; i < remoteForceToTrashDirectoriesList.size(); i++) {
      String item = remoteForceToTrashDirectoriesList.get(i);
      if (item.endsWith("/") && !item.equals("/")) {
        remoteForceToTrashDirectoriesList.set(i, item.substring(0, item.length() - 1));
      }
    }
    LOG.info("remoteForceToTrashDirectoriesList has {}.", remoteForceToTrashDirectoriesNums);
    remoteForceToTrashDirectories.addAll(remoteForceToTrashDirectoriesList);

    return remoteForceToTrashDirectories;
  }

  private static boolean checkLegality(List<String> remoteForceToTrashDirectoriesList) {
    for (String item : remoteForceToTrashDirectoriesList) {
      if (item.contains("/.Trash")) {
        LOG.warn("{} contain .Trash", item);
        return false;
      }
    }
    return true;
  }
}
