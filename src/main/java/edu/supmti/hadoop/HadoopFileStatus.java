package edu.supmti.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage HadoopFileStatus <file-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filepath = new Path(args[0]);
        if (!fs.exists(filepath)) {
            System.err.println("File does not exist: " + args[0]);
            System.exit(1);
        }

        FileStatus status = fs.getFileStatus(filepath);

        System.out.println("File Name: " + filepath.getName());
        System.out.println("File Size: " + status.getLen());
        System.out.println("File owner: " + status.getOwner());
        System.out.println("File Group: " + status.getGroup());

        BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            System.out.println("Block offset: " + blockLocation.getOffset());
            System.out.println("Block length: " + blockLocation.getLength());
            System.out.println("Hosts: " + String.join(",", blockLocation.getHosts()));
        }
    }
}
