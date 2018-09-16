package com.run.ycl.parser.file;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.ZipInputStream;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.ZipParameters;

import java.io.*;
import java.util.*;

/**
 * A <code>ZipFileDelegate</code> adds try-with-resource support to a
 * net.lingala.zip4j.core.ZipFile
 *
 * @author zhengd
 */
public class ZipFileDelegate implements Closeable, Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -42113370025611079L;
    private final ZipFile zipFile;
    private final Map<InputStream, String> streams = new WeakHashMap<>();
    private volatile boolean closeRequested = false;

    public ZipFileDelegate(File f) throws IOException {
	try {
	    this.zipFile = new ZipFile(f);
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public ZipFileDelegate(String filePath) throws IOException {
	try {
	    this.zipFile = new ZipFile(filePath);
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }
    
    public ZipFile getZipFile() {
		return zipFile;
	}

    public long length() {
	return this.zipFile.getFile().length();
    }

    public void createZipFile(File f) throws IOException {
	try {
	    zipFile.createZipFile(f, new ZipParameters());
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public void createZipFile(ArrayList<File> files) throws IOException {
	try {
	    zipFile.createZipFile(files, new ZipParameters());
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public void addFile(File f) throws IOException {
	try {
	    zipFile.addFile(f, new ZipParameters());
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public void addFiles(ArrayList<File> files) throws IOException {
	try {
	    zipFile.addFiles(files, new ZipParameters());
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public List<String> getFileNames() throws IOException {
	List<String> fileNames = new LinkedList<>();
	try {
	    List<FileHeader> headers = zipFile.getFileHeaders();
	    for (FileHeader fh : headers) {
		fileNames.add(fh.getFileName());
	    }
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
	return fileNames;
    }

    public boolean isEncrypted() throws IOException {
	try {
	    return zipFile.isEncrypted();
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public void setFileNameCharset(String charsetName) throws IOException {
	try {
	    zipFile.setFileNameCharset(charsetName);
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public long getLastModFileTime(String fileName) throws IOException {
	try {
	    return zipFile.getFileHeader(fileName).getLastModFileTime();
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public InputStream getFileInputStream(String fileName) throws IOException {
	InputStream stream;
	try {
	    stream = new ZipInputStreamDelegate(zipFile.getInputStream(zipFile
		    .getFileHeader(fileName)));
	    streams.put(stream, fileName);
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
	return stream;
    }

    public boolean isValidZipFile() {
	return zipFile.isValidZipFile();
    }

    public void setPassword(String password) throws IOException {
	try {
	    zipFile.setPassword(password);
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    public void setPassword(char[] password) throws IOException {
	try {
	    zipFile.setPassword(password);
	} catch (ZipException ex) {
	    throw new IOException(ex.getMessage(), ex);
	}
    }

    @Override
    public void close() throws IOException {
	if (closeRequested) {
	    return;
	} else {
	    closeRequested = true;
	}
	synchronized (streams) {
	    if (!streams.isEmpty()) {
		Map<InputStream, String> copy = new HashMap<>(streams);
		streams.clear();
		for (Map.Entry<InputStream, String> e : copy.entrySet()) {
		    e.getKey().close();
		}
	    }
	}
    }

    private class ZipInputStreamDelegate extends InputStream {

	private final ZipInputStream zis;
	private volatile boolean closeRequested = false;

	public ZipInputStreamDelegate(ZipInputStream zis) {
	    this.zis = zis;
	}

	@Override
	public void close() throws IOException {
	    if (closeRequested) {
		return;
	    }
	    closeRequested = true;
	    synchronized (ZipFileDelegate.this) {
		streams.remove(this);
		zis.close(true);
	    }
	}

	@Override
	public int read() throws IOException {
	    return zis.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
	    return zis.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
	    return zis.read(b, off, len);
	}

	public void close(boolean skipCRCCheck) throws IOException {
	    zis.close(skipCRCCheck);
	}

	@Override
	public int available() throws IOException {
	    return zis.available();
	}

	@Override
	public long skip(long n) throws IOException {
	    return zis.skip(n);
	}

    }
}
