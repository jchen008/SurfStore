package surfstore;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.protobuf.ByteString;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;


public final class TestFlight {
    private static final Logger logger = Logger.getLogger(TestFlight.class.getName());

    private final ManagedChannel leaderMetadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub leaderMetadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;
    private String action;
    private ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> nodeList;

    private ArrayList<String> localHashList;


    public TestFlight(ConfigReader config, String action) {

        this.leaderMetadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.leaderMetadataStub = MetadataStoreGrpc.newBlockingStub(leaderMetadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        if (config.getNumMetadataServers() > 1) {
            this.nodeList = new ArrayList<>();
            for (int i = 1; i <= config.getNumMetadataServers(); i++) {
                if (i != config.getLeaderNum()) {
                    ManagedChannel nodeMetadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i))
                            .usePlaintext(true).build();
                    MetadataStoreGrpc.MetadataStoreBlockingStub nodeMetadataStub = MetadataStoreGrpc.newBlockingStub(nodeMetadataChannel);
                    this.nodeList.add(nodeMetadataStub);
                }
            }
            System.out.println(this.nodeList.size() + " FOLLOWERS");
        }

        this.action = action;

        this.config = config;
    }


    public void shutdown() throws InterruptedException {
        leaderMetadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
	private void go() throws InterruptedException {
        leaderMetadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
        
        // TODO: Implement your client here
        FileInfo request;
        FileInfo FIResponse;
        Empty empty = Empty.newBuilder().build();
        WriteResult RWResponse;
        Block block;
        String testFilename1 = "abc.txt";
        String testFilename2 = "xyz.txt";
        String testFilename3 = "789.txt";
        String testFilename4 = "ultima.bin";
        String testFilename5 = "fin.fin";

        // 1. Test read new file
        request = FileInfo.newBuilder().setFilename(testFilename1).build();
        FIResponse = leaderMetadataStub.getVersion(request);
        assert FIResponse.getVersion() == 0 : "T1 getVersion error";
        System.out.println("TEST 1 PASS");


        // 2. Test upload and download
        block = Block.newBuilder().setHash("xyz").setData(ByteString.copyFrom("123456".getBytes())).build();
        blockStub.storeBlock(block);
        request = FileInfo.newBuilder().setFilename(testFilename2).addBlocklist("xyz").setVersion(1).build();
        RWResponse = leaderMetadataStub.modifyFile(request);
        assert RWResponse.getResult() == WriteResult.Result.OK : "T2 upload error";
        request = FileInfo.newBuilder().setFilename(testFilename2).build();
        FIResponse = leaderMetadataStub.readFile(request);
        assert FIResponse.getBlocklistList().get(0).equals("xyz") : "T2 download error";
        System.out.println("TEST 2 PASS");

        // 3. Delete
        request = FileInfo.newBuilder().setFilename(testFilename1).addBlocklist("xyz").setVersion(1).build();
        RWResponse = leaderMetadataStub.modifyFile(request);
        assert RWResponse.getResult() == WriteResult.Result.OK : "T3 upload error";
        RWResponse = leaderMetadataStub.deleteFile(request);
        assert RWResponse.getResult() == WriteResult.Result.OK : "T3 delete error";
        assert leaderMetadataStub.readFile(request).getBlocklist(0) == "0" : "T3 delete error";
        System.out.println("TEST 3 PASS");


        // 10. Test isLeader()
        assert leaderMetadataStub.isLeader(empty).getAnswer() : "T10 isLeader() error on Leader";
        for (MetadataStoreGrpc.MetadataStoreBlockingStub node : this.nodeList) {
            assert !node.isLeader(empty).getAnswer() : "T10 isLeader() error on Followers";
        }
        System.out.println("TEST 10 PASS");

        // 11. Test modify/delete on Followers
        for (MetadataStoreGrpc.MetadataStoreBlockingStub node : this.nodeList) {
            assert node.modifyFile(request).getResult() == WriteResult.Result.NOT_LEADER : "T11 Follower modifyFile exception";
            assert node.deleteFile(request).getResult() == WriteResult.Result.NOT_LEADER : "T11 Follower deleteFile exception";
        }
        System.out.println("TEST 11 PASS");

        // 12. Test Followers file version (normal)
        request = FileInfo.newBuilder().setFilename(testFilename2).build();
        for (MetadataStoreGrpc.MetadataStoreBlockingStub node : this.nodeList) {
            assert node.getVersion(request).getVersion() == 1 : "T12 Follower version mismatch (not crashed)";
        }
        System.out.println("TEST 12 PASS");

        // 20. Test crashed 1 Follower and upload (with majority)
        nodeList.get(0).crash(empty);
        assert nodeList.get(0).isCrashed(empty).getAnswer() : "T20 crash Follower exception";
        block = Block.newBuilder().setHash("abc").setData(ByteString.copyFrom("7890".getBytes())).build();
        blockStub.storeBlock(block);
        request = FileInfo.newBuilder().setFilename(testFilename3).addBlocklist("abc").setVersion(1).build();
        RWResponse = leaderMetadataStub.modifyFile(request);
        assert RWResponse.getResult() == WriteResult.Result.OK : "T20 upload error";
        assert leaderMetadataStub.getVersion(request).getVersion() == 1 : "T20 upload error";
        assert nodeList.get(1).getVersion(request).getVersion() == 1 : "T20 non-crashed Follower version mismatch";
        assert nodeList.get(0).getVersion(request).getVersion() == 0 : "T20 crashed Follower version mismatch";
        System.out.println("TEST 20 PASS");

        // 21. Test restored Follower updating log
        nodeList.get(0).restore(empty);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert nodeList.get(0).getVersion(request).getVersion() == 1 : "T20 restored Follower version mismatch";

        // 22. Test crashed 2 Followers and upload (fail)
        nodeList.get(0).crash(empty);
        nodeList.get(1).crash(empty);
        assert nodeList.get(1).isCrashed(empty).getAnswer() : "T22 crash Follower exception";
        request = FileInfo.newBuilder().setFilename(testFilename3).addBlocklist("abc").addBlocklist("xyz").setVersion(2).build();
        RWResponse = leaderMetadataStub.modifyFile(request);
        assert RWResponse.getResult() != WriteResult.Result.OK : "T22 upload without majority exception";
        nodeList.get(0).restore(empty);
        nodeList.get(1).restore(empty);
        System.out.println("TEST 22 PASS");

        // 30. Multi-threading 2PC
        System.out.println("TEST 30 BEGIN");
        Thread t0 = new Thread(new TFThread(leaderMetadataStub, testFilename4, "t0", 1));
        Thread t1 = new Thread(new TFThread(leaderMetadataStub, testFilename4, "t1", 1));
        blockStub.storeBlock(Block.newBuilder().setHash("t0").setData(ByteString.copyFrom("123456".getBytes())).build());
        blockStub.storeBlock(Block.newBuilder().setHash("t1").setData(ByteString.copyFrom("678901".getBytes())).build());
        t0.start();
        t1.start();
        t0.join();
        t1.join();
        FIResponse = leaderMetadataStub.readFile(FileInfo.newBuilder().setFilename(testFilename4).build());
        System.out.println("WINNER: " + FIResponse.getBlocklist(0));
        System.out.println("TEST 30 END");

        // 31. Test transmitting missing log
        nodeList.get(0).crash(empty);
        for (int i = 0; i < 10; i++) {
            request = FileInfo.newBuilder().setFilename(testFilename5).addBlocklist("xyz").setVersion(i + 1).build();
            leaderMetadataStub.modifyFile(request);
            assert leaderMetadataStub.getVersion(request).getVersion() == i + 1 : "T30 upload version mismatch";
        }
        Thread.sleep(3000);
        nodeList.get(0).restore(empty);
        Thread.sleep(2000);
        assert nodeList.get(0).getVersion(FileInfo.newBuilder().setFilename(testFilename5).build()).getVersion() == 10 : "T30 transmitting missing log fails";
        System.out.println("TEST 31 PASS");
	}

    class TFThread implements Runnable {
        private MetadataStoreGrpc.MetadataStoreBlockingStub leaderMetadataStub;
        private String filename;
        private String hash;
        private int version;

        public TFThread(MetadataStoreGrpc.MetadataStoreBlockingStub leaderMetadataStub, String filename, String hash, int version) {
            this.leaderMetadataStub = leaderMetadataStub;
            this.filename = filename;
            this.hash = hash;
            this.version = version;
        }
        public void run() {
            for (int i = 1; i <= 1; i++) {
                FileInfo request = FileInfo.newBuilder().setFilename(filename).addBlocklist(hash).setVersion(version).build();
                WriteResult response = leaderMetadataStub.modifyFile(request);
                System.out.println(hash + " " + response.getResult() + ". Version: " + response.getCurrentVersion());
                FileInfo FIResponse = leaderMetadataStub.readFile(FileInfo.newBuilder().setFilename(filename).build());
            }
        }
    }

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        TestFlight client = new TestFlight(config, c_args.getString("action"));

        try {
        	client.go();
        }
        finally {
            client.shutdown();
        }
    }
}
