package org.apache.cassandra.db.compaction;


import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.util.FileUtils;

@Ignore
public class LegacyLeveledManifestTestHelper extends SchemaLoader
{
    public final static String PROP = "migration-sstable-root";
    public final static String KS = "Keyspace1";
    public final static String CF = "legacyleveled";
    /**
     * Generates two sstables to be used to test migrating from a .json manifest to keeping the level in the sstable
     * metadata.
     *
     * Do this:
     * 1. remove @Ignore
     * 2. comment out the @Before and @After methods above
     * 3. run this method
     * 4. checkout trunk
     * 5. copy the .json file from the previous version to the current one
     *    (ie; test/data/migration-sstables/ic/Keyspace1/legacyleveled/legacyleveled.json)
     * 6. update LegacyLeveledManifestTest to use the new version.
     */
    @Test
    public void generateSSTable() throws IOException
    {
        File legacySSTableDir = getLegacySSTableDir(Descriptor.Version.current_version);
        FileUtils.createDirectory(legacySSTableDir);
        Set<String> keys = new HashSet<String>();
        for(int i = 0; i < 10; i++)
        {
            keys.add("key"+i);
        }
        for(int i = 0; i < 3; i++)
        {
            SSTableReader ssTable = SSTableUtils.prepare().ks(KS).cf(CF).dest(new Descriptor(legacySSTableDir, KS, CF, i, false)).write(keys);
            System.out.println(ssTable);
        }
    }
    public static File getLegacySSTableDir(String version)
    {
        return new File(System.getProperty(PROP) + File.separator + version + File.separator + KS + File.separator + CF);
    }

}
