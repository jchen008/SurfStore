package surfstore;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Arrays;

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


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;
    private String action;
    private File file;

    private ArrayList<String> localHashList;


    public Client(ConfigReader config, String action, File file) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.action = action;

        this.file = file;

        this.config = config;
    }

    private HashMap<String, Block> createFileBlocks(File f) throws IOException {
        HashMap<String, Block> blockMap = new LinkedHashMap<String, Block>();
        int chunkSize = 4096;

        FileInputStream fis = new FileInputStream(f);
        try {
            while (fis.available() > 0) {
                byte[] buffer = new byte[chunkSize];
                int numBytesRead = fis.read(buffer);
                if (numBytesRead != chunkSize) {
                    byte[] resizedBuffer = Arrays.copyOf(buffer, numBytesRead);
                    buffer = resizedBuffer;
                }
                String hash = shaSum(buffer);
                Block newBlock = Block.newBuilder()
                        .setHash(hash)
                        .setData(ByteString.copyFrom(buffer))
                        .build();
                this.localHashList.add(hash);
                blockMap.put(hash, newBlock);
            }
        }
        catch (FileNotFoundException e) {
            System.err.println("File not found" + e);
        }
        catch (IOException ioe) {
            System.err.println("Exception while reading file " + ioe);
        }
        finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            }
            catch (IOException ioe) {
                System.err.println("Error while closing stream: " + ioe);
            }
        }
        return blockMap;
    }

    private static String shaSum(byte[] b) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(b);
        String encoded = Base64.getEncoder().encodeToString(hash);

        return encoded;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
	private void go() {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
        
        // TODO: Implement your client here
        this.localHashList = new ArrayList<String>();
        HashMap<String, Block> localBlockMap;
        if (action.equals("upload")) {
            try {
                localBlockMap = createFileBlocks(this.file);
                FileInfo request = FileInfo.newBuilder()
                        .setFilename(this.file.getName())
                        .build();
                // readFile from MetadataStore, get version and block list
                FileInfo response = metadataStub.readFile(request);
                int storedVersion = response.getVersion();
                // case when file does not exist
                if (storedVersion == 0) {
                    // upload everything
                    for (String hash : localHashList){
                        Block block = localBlockMap.get(hash);
                        blockStub.storeBlock(block);
                    }
                    request = FileInfo.newBuilder()
                            .setFilename(this.file.getName())
                            .setVersion(1)
                            .addAllBlocklist(localHashList)
                            .build();
                    WriteResult result = metadataStub.modifyFile(request);
                    if (result.getResult() != WriteResult.Result.OK) {
                        System.err.println("Upload faied");
                        return;
                    }
                }
                // case when file needs to be updated
                else {
                    // get missing block list
                    request = FileInfo.newBuilder()
                            .setFilename(this.file.getName())
                            .setVersion(storedVersion + 1)
                            .addAllBlocklist(localHashList)
                            .build();
                    WriteResult firstResult = metadataStub.modifyFile(request);
                    // case when no change was applied
                    if (firstResult.getResult() == WriteResult.Result.OK) {

                    }
                    // case when missing blocks need to be uploaded
                    else if (firstResult.getResult() == WriteResult.Result.MISSING_BLOCKS) {
                        List<String> missingBlocksHashList = firstResult.getMissingBlocksList();
                        for (String hash : missingBlocksHashList) {
                            Block block = localBlockMap.get(hash);
                            while (!blockStub.hasBlock(block).getAnswer()) {
                                blockStub.storeBlock(block);
                            }
                        }
                        WriteResult secondResult = metadataStub.modifyFile(request);
                        if (secondResult.getResult() != WriteResult.Result.OK) {
                            System.err.println("Upload failed");
                        }
                    }
                }
                System.out.println("OK");
            }
            catch(IOException e) {
                logger.severe(e.getMessage());
            }
        }
        else if (action.equals("download")) {
            FileInfo request = FileInfo.newBuilder().setFilename(this.file.getName()).build();
            FileInfo response = metadataStub.readFile(request);
            int storedVersion = response.getVersion();
            List<String> remoteHashList = response.getBlocklistList();
            if (storedVersion == 0) {
                System.err.println("Not Found");
                return;
            }
            if (remoteHashList.size() == 1 && remoteHashList.get(0).equals("0")) {
                System.err.println("Download failed. File was deleted.");
                return;
            }
            if (this.file.isDirectory()) {
                System.err.println("Download failed. Dir exists.");
                return;
            }
            // File exist on local FS, download missing blocks
            if (this.file.exists()) {
                try {
                    localBlockMap = createFileBlocks(this.file);
                    localHashList = new ArrayList<>(localBlockMap.keySet());
                    ArrayList<String> missingBlockList = new ArrayList<String>();
                    for (String hash : remoteHashList) {
                        if (!localHashList.contains(hash)) {
                            missingBlockList.add(hash);
                        }
                    }
                    HashMap<String, Block> missingBlockMap = new HashMap<String, Block>();
                    for (String missingBlockHash : missingBlockList) {
                        Block blockRequest = Block.newBuilder().setHash(missingBlockHash).build();
                        Block blockResponse = blockStub.getBlock(blockRequest);
                        missingBlockMap.put(missingBlockHash, blockResponse);
                    }
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    for (String hash : remoteHashList) {
                        if (localBlockMap.containsKey(hash)) {
                            byteArrayOutputStream.write(localBlockMap.get(hash).getData().toByteArray());
                        }
                        else if (missingBlockMap.containsKey(hash)) {
                            byteArrayOutputStream.write(missingBlockMap.get(hash).getData().toByteArray());
                        }
                        else {
                            System.err.println("Missing block");
                        }
                    }
                    FileOutputStream fileOutputStream = new FileOutputStream(this.file);
                    byteArrayOutputStream.writeTo(fileOutputStream);
                    fileOutputStream.close();
                }
                catch (IOException e) {
                    logger.severe(e.getMessage());
                }
            }
            // File does not exist, download whole file
            else {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                for (String hash : remoteHashList) {
                    Block blockRequest = Block.newBuilder().setHash(hash).build();
                    Block blockResponse = blockStub.getBlock(blockRequest);
                    try {
                        byteArrayOutputStream.write(blockResponse.getData().toByteArray());
                    }
                    catch (IOException e){
                        logger.severe(e.getMessage());
                    }
                }
                try {
                    FileOutputStream fileOutputStream = new FileOutputStream(this.file);
                    byteArrayOutputStream.writeTo(fileOutputStream);
                    fileOutputStream.close();
                }
                catch (FileNotFoundException e) {
                    logger.severe(e.getMessage());
                }
                catch (IOException e) {
                    logger.severe(e.getMessage());
                }
            }
            System.out.println("OK");
        }
        else if (action.equals("delete")) {
            FileInfo request = FileInfo.newBuilder().setFilename(this.file.getName()).build();
            FileInfo response = metadataStub.readFile(request);
            int storedVersion = response.getVersion();
            if (storedVersion == 0) {
                System.err.println("Not Found");
            }
            else {
                request = FileInfo.newBuilder()
                        .setFilename(this.file.getName())
                        .setVersion(storedVersion + 1)
                        .build();
                WriteResult result = metadataStub.deleteFile(request);
                if (result.getResult() != WriteResult.Result.OK) {
                    System.err.println("Delete failed.");
                }
                else {
                    System.out.println("OK");
                }
            }
        }
        else if (action.equals("getversion")) {
            FileInfo request = FileInfo.newBuilder().setFilename(this.file.getName()).build();
            int version = metadataStub.readFile(request).getVersion();
            System.out.println(version);
        }
        else {
            System.err.println("invalid action");
            System.exit(1);
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
        parser.addArgument("action").type(String.class)
                .help("User specified action on target file");
        parser.addArgument("file").type(String.class)
                .help("Target file");
        parser.addArgument("download_path").nargs("?").type(String.class).setDefault("").required(false)
                .help("Path to downloads");
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

        File file = new File(c_args.getString("file"));
        if (c_args.getString("action").equals("download")) {
            File dir = new File(c_args.getString("download_path"));
            if (dir.isDirectory()) {
                file = new File(dir.getAbsoluteFile(), c_args.getString("file"));
                System.out.println("D2: " + file.getAbsolutePath());
            }
            else {
                System.err.println("Invalid download directory");
            }
        }
        Client client = new Client(config, c_args.getString("action"), file);

        try {
        	client.go();
        }
        finally {
            client.shutdown();
        }
    }
}
