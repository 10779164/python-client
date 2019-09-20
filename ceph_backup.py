#!encoding=utf8
# !/usr/bin/python
from executor import execute
import re
import time
import argparse
import ConfigParser
import logging
import os
import rados
import rbd
from kubernetes import client, config



class Logger():
    def __init__(self, logfile, loglevel, logger):
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s -- %(name)s -- %(levelname)s --- %(message)s')
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    def getlog(self):
        return self.logger


class CephBackup(object):
    '''
    rbd commands:
    rbd snap create rbd/image@snap_name
    rbd export-diff --from snap newest_snap rbd/image@cur_snap /path/filename
    '''
    PREFIX = 'SNAPSHOT'
    TIME_FMT = time.strftime('%Y%m%d%H%M%S')
    SNAPSHOT_NAME = '{}-{}'.format(PREFIX, TIME_FMT)

    def __init__(self, pool, namespace, backup_dest, ceph_conf, backup_init):
        super(CephBackup, self).__init__()
        self._pool = pool
        self._namespace = namespace
        self._backup_dest = backup_dest
        self._ceph_conf = ceph_conf
        self._backup_init = backup_init

        try:
            cluster = rados.Rados(conffile=ceph_conf)
            cluster.connect()
        except:
            raise Exception('Unable to connect to ceph cluster')
        else:
            self._ceph_ioctx = cluster.open_ioctx(pool)
            self._ceph_rbd = rbd.RBD()
            self._image = self._get_image()


    def _get_image(self):
        # return a list
        #return self._ceph_rbd.list(self._ceph_ioctx)
        config.load_kube_config('./config')
        v1 = client.CoreV1Api()
	for i in v1.list_namespaced_persistent_volume_claim(namespace=self._namespace).items:
            volume=i.spec.volume_name
        for i in v1.list_persistent_volume().items:
            if i.metadata.name == volume:
		image=i.spec.rbd.image
                break
	return image

    '''
    imagename:
    for imagename in self._images:
	 pass
    '''

    def _get_snapshots(self, imagename):
        # return a dic
        # {'namespace': 0, 'id': 266L, 'name': u'SNAPSHOT-20190530004929', 'size': 1073741824L}
        # dic.get('name') return snapshot name
        prefix_length = len(CephBackup.PREFIX)
        image = rbd.Image(self._ceph_ioctx, imagename)
        snapshots = []
        for snapshot in image.list_snaps():
            if snapshot.get('name')[0:prefix_length] == CephBackup.PREFIX:
                snapshots.append(snapshot.get('name'))
        return snapshots

    def _get_num_snapshosts(self, imagename):
        return len(self._get_snapshots(imagename))

    def _get_newest_snapshot(self, imagename):
        snapshots = self._get_snapshots(imagename)
        if len(snapshots) is 0:
            return None
        return max(snapshots)

    def _get_oldest_snapshot(self, imagename):
        snapshots = self._get_snapshots(imagename)
        if len(snapshots) is 0:
            return None
        return min(snapshots)

    def _create_snapshot(self, imagename):
        image = rbd.Image(self._ceph_ioctx, imagename)
        image.create_snap(CephBackup.SNAPSHOT_NAME)
        return CephBackup.SNAPSHOT_NAME

    def _backup_init_whether(self, imagename):
        num = int(self._backup_init) + 1
        num_snapshots = int(self._get_num_snapshosts(imagename))
        if num_snapshots >= num:
            return False
        else:
            return True

    def _delete_overage_snapshot(self, imagename, full_snapname):
        snapshots = self._get_snapshots(imagename)
        image = rbd.Image(self._ceph_ioctx, imagename)
        for overage_snapshot in snapshots:
            if overage_snapshot != full_snapname:
                print "Deleting snapshot {pool}/{snapname}".format(pool=self._pool, snapname=overage_snapshot)
                image.remove_snap(overage_snapshot)

    def _delete_overage_backupfile(self, imagename):
        # snapshots = self._get_snapshots(imagename)
        dest_dir = os.path.join(self._backup_dest, self._pool, imagename)
        for dest_file in os.listdir(dest_dir):
            if re.match(r"{image}-(.*?)".format(image=imagename), dest_file):
                backup_file = dest_dir + '/' + dest_file
                print "Deleting backup file {backup_file}".format(backup_file=dest_file)
                os.remove(backup_file)

    def _export_full_backupfile(self, imagename):
        filename = imagename + '-' + CephBackup.SNAPSHOT_NAME + ".full"
        dest_dir = os.path.join(self._backup_dest, self._pool, imagename)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        full_filename = dest_dir + '/' + filename
        execute("rbd export {pool}/{image} {dest}".format(pool=self._pool, image=imagename, dest=full_filename),
                sudo=True)
        print "Exporting image {pool}/{image} to {dest}\n".format(pool=self._pool, image=imagename, dest=full_filename)

    def _export_diff_backupfile(self, imagename, newest_snapshot, cur_snapshot):
        filename = imagename + '-' + cur_snapshot + ".diff_from_" + newest_snapshot
        dest_dir = os.path.join(self._backup_dest, self._pool, imagename)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        full_filename = dest_dir + '/' + filename
        execute("rbd export-diff --from-snap {base} {pool}/{image}@{cur} {dest}".format(base=newest_snapshot,
                                                                                        pool=self._pool,
                                                                                        image=imagename,
                                                                                        cur=cur_snapshot,
                                                                                        dest=full_filename), sudo=True)
        print "Exporting image {pool}/{image} to {dest}\n".format(pool=self._pool, image=imagename, dest=full_filename)

    def _incremental_init_backup(self, imagename):
        print "\033[0;36m" + "Starting incremental init backup for {image}:".format(image=imagename) + "\033[0m"
        self._create_snapshot(imagename)
        self._export_full_backupfile(imagename)

    def _incremental_full_backup(self, imagename):
        # create full snapshot --> delete overage snapshot --> delete backup export file --> export full snapshot to backup dir
        self._create_snapshot(imagename)
        full_snapname = self._get_newest_snapshot(imagename)
        self._delete_overage_snapshot(imagename, full_snapname)
        self._delete_overage_backupfile(imagename)
        self._export_full_backupfile(imagename)

    '''
    def full_backup(self):
	#create full snapshot --> delete overage snapshot --> delete backup export file --> export full snapshot to backup dir
	print "Starting full backup..."
	for imagename in self._images:
	    print "\033[0;36m"+"{pool}/{image}:".format(pool=self._pool,image=imagename)+"\033[0m"
	    #create full backup snapshot        
            self._create_snapshot(imagename)

	    #delete overage snapshot and export backup file
	    if self._get_num_snapshosts(imagename) != 1 and self._backup_mode == "incremental":
	        full_snapname=self._get_newest_snapshot(imagename)
	        self._delete_overage_snapshot(imagename,full_snapname)
                self._delete_overage_backupfile(imagename)
		self._export_full_backupfile(imagename)

	    else:
	    #export full backup	
	        self._export_full_backupfile(imagename)
    '''

    # Ceph rbd full backup
    def full_backup(self):
        '''
            create full snapshot --> export full snapshot to backup dir
            '''
        print "Starting full backup..."
        imagename=self._image
        print "\033[0;36m" + "{pool}/{image}:".format(pool=self._pool, image=imagename) + "\033[0m"
        self._create_snapshot(imagename)
        self._export_full_backupfile(imagename)

    # Ceph rbd incremental backup
    def incremental_backup(self):
        '''
        num of snapshot (> backup init) --> incremental_full_backup
                    (< backup init) --> get newest_snapshot --> create cur_snapshot --> export diff-from newest_snapshot image@cur_snapshot file
        '''
        print "Starting increment backup..."
        imagename=self._image
        if self._get_num_snapshosts(imagename) == 0:
            self._incremental_init_backup(imagename)
        else:
            m = self._backup_init_whether(imagename)
            if m:
                try:
                    print "\033[0;36m" + "Starting incremental backup for {image}:".format(
                        image=imagename) + "\033[0m"

                    # get current newest snapshot
                    newest_snapshot = self._get_newest_snapshot(imagename)

                    # create snapshot
                    cur_snapshot = self._create_snapshot(imagename)

                    # export diff file
                    self._export_diff_backupfile(imagename, newest_snapshot, cur_snapshot)
                except Exception, e:
                    logger.error(str(e))
                else:
                    logger.info('Image {}/{} backup successful'.format(self._pool, imagename))
                # logger.info('OK')
            else:
                print "\033[0;36m" + "Starting new full backup for {image}:".format(image=imagename) + "\033[0m"
                try:
                    self._incremental_full_backup(imagename)
                except Exception, e:
                    logger.error(str(e))
                else:
                    logger.info('Image {}/{} full backup successful'.format(self._pool, imagename))


class Settings(object):
    def __init__(self, path, namespace):
        '''
        path: path to the configuration file
        '''
        super(Settings, self).__init__()
        self.namespace=namespace
        self._path = path
        if not os.path.exists(path):
            raise Exception('Configuration file not found: {}'.format(path))
        self._config = ConfigParser.ConfigParser()
        self._config.read(self._path)

    def getsetting(self, section, setting):
        return self._config.get(section, setting)

    def start_backup(self):
        '''
        Read settings and starts backup
        '''
        global logger
        logger = Logger(logfile='/var/log/ceph_backup.log', loglevel=1, logger="ceph rbd backup").getlog()
        for section in self._config.sections():
            print "Starting backup for pool {}".format(section)
	    namespace=self.namespace
            backup_dest = self.getsetting(section, 'backup directory')
            conf_file = self.getsetting(section, 'ceph config')
            backup_init = self.getsetting(section, 'backup init')
            backup_mode = self.getsetting(section, 'backup mode')
            cb = CephBackup(section, namespace, backup_dest, conf_file, backup_init)
            if backup_mode == 'full':
                cb.full_backup()
            elif backup_mode == 'incremental':
                cb.incremental_backup()
            else:
                raise Exception("Unknown backup mode: {}".format(backup_mode))


def test():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pool', help="Ceph rbd pool name", required=False)
    parser.add_argument('-i', '--images', nargs='+', help="List of ceph images to backup", required=False)
    parser.add_argument('-d', '--dest', help="Backup file directory", required=False)
    parser.add_argument('-c', '--ceph-conf', help="Path to ceph configuration file", type=str,
                        default='/etc/ceph/ceph.conf')
    args = parser.parse_args()

    cb = CephBackup(args.pool, args.images, args.dest, args.ceph_conf, args.backup_mode)
    cb.incremental_backup()


def main():
    # cb=CephBackup("rbd","*","/backup/CephBackup/","/etc/ceph/ceph.conf",3)
    # cb.full_backup()
    # cb.incremental_backup()
    # global logger
    # logger=Logger(logfile='/var/log/ceph_backup.log', loglevel=1, logger="ceph rbd backup").getlog()

    #namespace=execute('cat /run/secrets/kubernetes.io/serviceaccount/namespace')
    namespace='wordpress-004'
    parser = argparse.ArgumentParser()
    default_cephbackup_cfg = '/etc/cephbackup/cephbackup.conf'
    parser.add_argument('-p', '--pool', help="Ceph rbd pool name", required=False)
    parser.add_argument('-d', '--dest', help="Backup file directory", required=False)
    parser.add_argument('-c', '--conf',
                        help="path to the configuration file (default: {})".format(default_cephbackup_cfg), type=str,
                        default=default_cephbackup_cfg)
    args = parser.parse_args()
    settings = Settings(args.conf,namespace)
    settings.start_backup()


if __name__ == '__main__':
    main()

