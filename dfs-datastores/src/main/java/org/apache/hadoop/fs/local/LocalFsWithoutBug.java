package org.apache.hadoop.fs.local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hack to fix LocalFs implementation.
 * This has to be in the same package of LocalFs due to package private constructors.
 */
public class LocalFsWithoutBug extends LocalFs {
    LocalFsWithoutBug(Configuration conf) throws IOException, URISyntaxException {
        super(conf);
    }

    LocalFsWithoutBug(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, conf);
    }

    // this method is private in ChecksumFs so copied here
    private boolean isDirectory(Path f) throws IOException, UnresolvedLinkException {
        try {
            return this.getMyFs().getFileStatus(f).isDirectory();
        } catch (FileNotFoundException var3) {
            return false;
        }
    }

    // this method is private in ChecksumFs so copied here
    private boolean exists(Path f) throws IOException, UnresolvedLinkException {
        try {
            return this.getMyFs().getFileStatus(f) != null;
        } catch (FileNotFoundException var3) {
            return false;
        }
    }

    /**
     * This method is missing in ChecksumFs so it was using the implementation provided in FilterFs and
     * did not move crc files.
     *
     * @param src source path
     * @param dst destination path
     * @param overwrite overwrite flag
     */
    @Override
    public void renameInternal(Path src, Path dst, boolean overwrite) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, AccessControlException, IOException {
        Options.Rename opt = overwrite ? Options.Rename.OVERWRITE : Options.Rename.NONE;
        if (isDirectory(src)) {
            getMyFs().rename(src, dst, opt);
        } else {
            getMyFs().rename(src, dst, opt);

            Path checkFile = getChecksumFile(src);
            if (exists(checkFile)) { //try to rename checksum
                if (isDirectory(dst)) {
                    getMyFs().rename(checkFile, dst, opt);
                } else {
                    getMyFs().rename(checkFile, getChecksumFile(dst), opt);
                }
            }
        }
    }
}
