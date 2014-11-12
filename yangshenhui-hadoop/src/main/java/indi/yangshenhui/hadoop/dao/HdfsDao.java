package indi.yangshenhui.hadoop.dao;

import indi.yangshenhui.hadoop.dao.exception.HdfsDaoException;
import indi.yangshenhui.tool.CloseUtil;
import indi.yangshenhui.tool.CollectionUtil;
import indi.yangshenhui.tool.StringUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * 
 * @author yangshenhui
 */
public class HdfsDao {
	private static final String HDFS = "hdfs://redhat:9000/";
	private static final Logger LOGGER = Logger.getLogger(HdfsDao.class);
	private String hdfsPath;
	private Configuration configuration;

	public HdfsDao(Configuration configuration) {
		this(HDFS, configuration);
	}

	public HdfsDao(String hdfsPath, Configuration configuration) {
		this.hdfsPath = hdfsPath;
		this.configuration = configuration;
	}

	public boolean makeDirectory(String directory) throws HdfsDaoException {
		if (StringUtil.isEmpty(directory)) {
			return false;
		}
		LOGGER.info(String
				.format("start create hdfs directory [%s]", directory));
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
			Path path = new Path(directory);
			if (fileSystem.exists(path)) {
				LOGGER.info(String.format(
						"hdfs directory [%s] already exists can not create",
						directory));
				return false;
			}
			fileSystem.mkdirs(path);
			LOGGER.info(String.format("hdfs directory [%s] create success",
					directory));
			LOGGER.info(String.format("end create hdfs directory [%s]",
					directory));
		} catch (IOException e) {
			throw new HdfsDaoException(e);
		} finally {
			CloseUtil.close(fileSystem);
		}
		return true;
	}

	public boolean removeDirectory(String directory) throws HdfsDaoException {
		if (StringUtil.isEmpty(directory)) {
			return false;
		}
		LOGGER.info(String
				.format("start remove hdfs directory [%s]", directory));
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
			Path path = new Path(directory);
			fileSystem.deleteOnExit(path);
			LOGGER.info(String.format("hdfs directory [%s] remove success",
					directory));
			LOGGER.info(String.format("end remove hdfs directory [%s]",
					directory));
		} catch (IOException e) {
			throw new HdfsDaoException(e);
		} finally {
			CloseUtil.close(fileSystem);
		}
		return true;
	}

	public List<Path> listDirectory(String directory) throws HdfsDaoException {
		if (StringUtil.isEmpty(directory)) {
			return Collections.emptyList();
		}
		LOGGER.info(String.format("start list hdfs directory [%s]", directory));
		FileSystem fileSystem = null;
		List<Path> pathList = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
			Path path = new Path(directory);
			if (!fileSystem.exists(path)) {
				LOGGER.info(String.format("hdfs directory [%s] does not exist",
						directory));
				return null;
			}
			pathList = new ArrayList<Path>();
			list(pathList, fileSystem, path);
			LOGGER.info(String
					.format("end list hdfs directory [%s]", directory));
		} catch (IOException e) {
			throw new HdfsDaoException(e);
		} finally {
			CloseUtil.close(fileSystem);
		}
		return pathList;
	}

	private void list(List<Path> fileList, FileSystem fileSystem, Path path)
			throws FileNotFoundException, IOException {
		FileStatus[] fileStatuses = fileSystem.listStatus(path);
		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.isDirectory()) {
				list(fileList, fileSystem, fileStatus.getPath());
			} else {
				fileList.add(fileStatus.getPath());
			}
		}
	}

	public boolean create(String fileName, List<String> contentList)
			throws HdfsDaoException {
		if (StringUtil.isEmpty(fileName)) {
			return false;
		}
		LOGGER.info(String.format("start create hdfs file [%s]", fileName));
		FileSystem fileSystem = null;
		FSDataOutputStream fsDataOutputStream = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
			Path path = new Path(fileName);
			if (fileSystem.exists(path)) {
				LOGGER.info(String.format(
						"hdfs file [%s] already exists can not create",
						fileName));
				return false;
			}
			fsDataOutputStream = fileSystem.create(path);
			if (CollectionUtil.isNotEmpty(contentList)) {
				for (String content : contentList) {
					byte[] buffer = content.getBytes();
					fsDataOutputStream.write(buffer, 0, buffer.length);
					fsDataOutputStream.write("\n".getBytes());
				}
			}
			LOGGER.info(String.format("end create hdfs file [%s]", fileName));
		} catch (IOException e) {
			throw new HdfsDaoException(e);
		} finally {
			CloseUtil.close(fsDataOutputStream);
			CloseUtil.close(fileSystem);
		}
		return true;
	}

	public boolean upload(String localFile, String remoteDirectory)
			throws HdfsDaoException {
		if (StringUtil.isEmpty(localFile)
				|| StringUtil.isEmpty(remoteDirectory)) {
			return false;
		}
		LOGGER.info("start upload file [" + localFile + "] to hdfs directory ["
				+ remoteDirectory + "]");
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
			fileSystem.copyFromLocalFile(new Path(localFile), new Path(
					remoteDirectory));
			LOGGER.info(String.format(
					"upload local file [%s] to hdfs directory [%s]", localFile,
					remoteDirectory));
			LOGGER.info("end upload file [" + localFile
					+ "] to hdfs directory [" + remoteDirectory + "]");
		} catch (IOException e) {
			throw new HdfsDaoException(e);
		} finally {
			CloseUtil.close(fileSystem);
		}
		return true;
	}

	public boolean download(String remoteFile, String localDirectory)
			throws HdfsDaoException {
		if (StringUtil.isEmpty(remoteFile)
				|| StringUtil.isEmpty(localDirectory)) {
			return false;
		}
		LOGGER.info("start download hdfs file [" + remoteFile
				+ "] to local directory [" + localDirectory + "]");
		FileSystem fileSystem = null;
		FSDataInputStream fsDataInputStream = null;
		OutputStream outputStream = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
			fsDataInputStream = fileSystem.open(new Path(remoteFile));
			localDirectory = localDirectory + File.separator
					+ extractFileName(remoteFile);
			outputStream = new FileOutputStream(new File(localDirectory));
			IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, false);
			LOGGER.info(String.format(
					"download hdfs file [%s] to local directory [%s]",
					remoteFile, localDirectory));
			LOGGER.info("end download hdfs file [" + remoteFile
					+ "] to local directory [" + localDirectory + "]");
		} catch (IOException e) {
			throw new HdfsDaoException(e);
		} finally {
			CloseUtil.close(outputStream);
			CloseUtil.close(fsDataInputStream);
			CloseUtil.close(fileSystem);
		}
		return true;
	}

	private String extractFileName(String filePath) {
		return StringUtils.substring(filePath,
				StringUtils.lastIndexOf(filePath, "/") + 1, filePath.length());
	}

}