package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

public class BzlNoPermissionCheckINodeAttributesProvider extends INodeAttributeProvider {

  public static class BzlNoPermissionCheckControlEnforcer implements AccessControlEnforcer {

    public BzlNoPermissionCheckControlEnforcer() {
    }

    @Override
    public void checkPermission(String fsOwner, String supergroup,
                                UserGroupInformation ugi, INodeAttributes[] inodeAttrs,
                                INode[] inodes, byte[][] pathByNameArr, int snapshotId,
                                String path,
                                int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
                                FsAction parentAccess, FsAction access, FsAction subAccess,
                                boolean ignoreEmptyDir) throws AccessControlException {
    }

    @Override
    public void checkPermissionWithContext(
        AuthorizationContext authzContext) throws AccessControlException {
    }
  }

  @Override
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return new BzlNoPermissionCheckControlEnforcer();
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public INodeAttributes getAttributes(String[] pathElements, INodeAttributes inode) {
    return inode;
  }

}
