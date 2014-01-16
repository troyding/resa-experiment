/**
 *
 */
package storm.resa.metric;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Wraps a ZooKeeper instance and adds iSimilar specific functionality.
 *
 * @author troy 2010-1-25
 */
public class ZooKeeperFacade {

    public interface ZkExpiredHandler {
        public void handle();
    }

    private static final Logger LOG = Logger.getLogger(ZooKeeperFacade.class);

    private final ZooKeeper zk;

    private class DefaultWatcher implements Watcher {
        private CountDownLatch countSignal = new CountDownLatch(1);

        @Override
        public void process(WatchedEvent event) {
            switch (event.getState()) {
                case SyncConnected:
                    countSignal.countDown();
                    break;

                case Expired:
                    LOG.fatal("ZooKeeper Expired ******");
                    break;

                default:
                    break;
            }
        }

        public void waitForConnection() throws InterruptedException {
            countSignal.await();
        }
    }

    public ZooKeeperFacade(ZooKeeper zk) {
        this.zk = zk;
    }

    public ZooKeeperFacade(String connectString, int timeout) {
        DefaultWatcher defaultZkWatcher = new DefaultWatcher();
        try {
            this.zk = new ZooKeeper(connectString, timeout, defaultZkWatcher);
            defaultZkWatcher.waitForConnection();
        } catch (Exception e) {
            throw new RuntimeException("Create zoo keeper instance failed", e);
        }
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
        }
    }

    /* add a zk watcher on a path */
    public boolean watchZNode(String zkPath, Watcher watcher) {
        boolean success = false;
        try {
            zk.exists(zkPath, watcher);
            success = true;
        } catch (Exception e) {
            LOG.info("Failed to add watch on ZNode " + zkPath, e);
        }
        return success;
    }

    /* Check path exist */
    public boolean znodeExist(String zkPath, Watcher watcher) {
        try {
            return zk.exists(zkPath, watcher) != null;
        } catch (Exception e) {
            LOG.info("Failed to check exist on ZNode " + zkPath, e);
        }
        return false;
    }

    /* Check path exist */
    public boolean znodeDelete(String zkPath) {
        try {
            zk.delete(zkPath, -1);
            return true;
        } catch (KeeperException.NoNodeException e) {
            return true;
        } catch (Exception e) {
            LOG.info("Failed to delete ZNode " + zkPath, e);
        }
        return false;
    }

    public boolean znodeDeleteRecursive(String znode) {
        try {
            znodeDeleteRecursive0(znode);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private void znodeDeleteRecursive0(String znode) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(znode, false);
        if (stat != null) {
            if (stat.getNumChildren() == 0) {
                try {
                    zk.delete(znode, -1);
                } catch (KeeperException.NoNodeException e) {
                } catch (KeeperException.NotEmptyException e) {
                    znodeDeleteRecursive0(znode);
                }
            } else {
                List<String> children = zk.getChildren(znode, false);
                for (String child : children) {
                    znodeDeleteRecursive0(joinPath(znode, child));
                }
            }
        }
    }

    /* Get data on a path */
    public byte[] znodeData(String zkPath, Watcher watcher) {
        try {
            return zk.getData(zkPath, watcher, null);
        } catch (Exception e) {
            LOG.info("Failed get data on ZNode " + zkPath, e);
        }
        return null;
    }

    /* Get child on a path */
    public List<String> znodeChildren(String zkPath, Watcher watcher) {
        try {
            return zk.getChildren(zkPath, watcher);
        } catch (Exception e) {
            LOG.info("Failed get data on ZNode " + zkPath, e);
        }
        return null;
    }

    /* add a zk watcher on a path */
    public boolean watchZChildren(String zkPath, Watcher watcher) {
        boolean success = false;
        try {
            zk.getChildren(zkPath, watcher);
            success = true;
        } catch (Exception e) {
            LOG.info("Failed to add watch on ZNode " + zkPath, e);
        }
        return success;
    }

    /* add a zk watcher on a path */
    public boolean createZNode(String zkPath, byte[] data, CreateMode createMode) {
        try {
            zk.create(zkPath, data, Ids.OPEN_ACL_UNSAFE, createMode);
            return true;
        } catch (Exception e) {
            LOG.info("Failed to add create on ZNode " + zkPath, e);
        }
        return false;
    }

    /* add a zk watcher on a path */
    public boolean setZNodeData(String zkPath, byte[] data) {
        try {
            zk.setData(zkPath, data, -1);
            return true;
        } catch (Exception e) {
            LOG.info("Failed to add create on ZNode " + zkPath, e);
        }
        return false;
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    /* Join two path */
    public static String joinPath(String parent, String child) {
        boolean parentEndOfSplit = parent.endsWith("/");
        boolean childStartOfSplit = child.startsWith("/");
        if (childStartOfSplit && parentEndOfSplit) {
            return parent + '/' + child.substring(1);
        } else if (!childStartOfSplit && !parentEndOfSplit) {
            return parent + '/' + child;
        }
        return parent + child;
    }

}
