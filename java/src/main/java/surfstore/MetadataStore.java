package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.HashMap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.SimpleAnswer;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
	protected Namespace c_args;

    public MetadataStore(Namespace c_args, ConfigReader config) {
        this.c_args = c_args;
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(c_args, config))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }


    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        private ArrayList<FileInfo> fileInfoList;
        private HashMap<String, FileInfo> fileSystemMap;
        private ArrayList<FileInfo> confirmedTransactions;
        private ArrayList<FileInfo> pendingTransactions;
        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        private final boolean isLeader;
        private ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> nodeList;
        private ReentrantLock lock0;
        private ReentrantLock lock1;
        private boolean crashed;
        private AppendLogTimerTask timerTask;

        MetadataStoreImpl(Namespace c_args, ConfigReader config) {
            this.blockChannel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", config.getBlockPort())
                    .usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            this.lock0 = new ReentrantLock();
            this.lock1 = new ReentrantLock();
            fileSystemMap = new HashMap<String, FileInfo>();
            confirmedTransactions = new ArrayList<>();
            pendingTransactions = new ArrayList<>();

            // Check leadership
            if (c_args.getInt("number") == config.getLeaderNum()){
                isLeader = true;
                nodeList = new ArrayList<>();
                // Add followers to nodeList
                for (int i = 1; i <= config.getNumMetadataServers(); i++) {
                    if (i != config.getLeaderNum()) {
                        ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i)).usePlaintext(true).build();
                        MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
                        nodeList.add(metadataStub);
                    }
                }
                timerTask = new AppendLogTimerTask();
                Timer timer = new Timer(true);
                timer.scheduleAtFixedRate(timerTask, 0, 500);
            }
            else {
                isLeader = false;
            }
            this.crashed = false;
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!

        /**
         * <pre>
         * Read the requested file.
         * The client only needs to supply the "filename" argument of FileInfo.
         * The server only needs to fill the "version" and "blocklist" fields.
         * If the file does not exist, "version" should be set to 0.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * </pre>
         */
        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            if (!isLeader) {
                System.err.println("Illegal readFile() call on non-leader node.");
                return;
            }
            String filename = request.getFilename();
            FileInfo response;
            if (fileSystemMap.keySet().contains(filename)) {
                response = FileInfo.newBuilder().mergeFrom(fileSystemMap.get(filename)).build();
            } else {
                response = FileInfo.newBuilder()
                        .setFilename(filename)
                        .setVersion(0)
                        .build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Write a file.
         * The client must specify all fields of the FileInfo message.
         * The server returns the result of the operation in the "result" field.
         *
         * The server ALWAYS sets "current_version", regardless of whether
         * the command was successful. If the write succeeded, it will be the
         * version number provided by the client. Otherwise, it is set to the
         * version number in the MetadataStore.
         *
         * If the result is MISSING_BLOCKS, "missing_blocks" contains a
         * list of blocks that are not present in the BlockStore.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * </pre>
         */
        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            lock0.lock();
            WriteResult response;
            String filename = request.getFilename();
            int storedFileVersion = 0;

            // File exists on MetadataStore
            if (fileSystemMap.containsKey(filename)) {
                storedFileVersion = fileSystemMap.get(filename).getVersion();
            }

            // Check if server is leader
            if (!isLeader) {
                response = WriteResult.newBuilder().setResult(WriteResult.Result.NOT_LEADER).setCurrentVersion(storedFileVersion).build();
            }

            // Version mismatch
            else if (request.getVersion() != storedFileVersion + 1) {
                response = WriteResult.newBuilder().setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(storedFileVersion).build();
            }

            // Version OK
            else {
                ArrayList<String> missingBlockList = new ArrayList<String>();
                for (String blockHash : request.getBlocklistList()) {
                    Block block = Block.newBuilder().setHash(blockHash).build();
                    if (!blockStub.hasBlock(block).getAnswer()) {
                        missingBlockList.add(blockHash);
                    }
                }
                // Missing blocks
                if (missingBlockList.size() > 0) {
                    response = WriteResult.newBuilder()
                            .setResult(WriteResult.Result.MISSING_BLOCKS)
                            .setCurrentVersion(storedFileVersion)
                            .addAllMissingBlocks(missingBlockList)
                            .build();
                }
                // Ready to modify
                else {
                    response = commitTransaction(request);
                }
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            lock0.unlock();
        }

        /**
         * <pre>
         * Delete a file.
         * This has the same semantics as ModifyFile, except that both the
         * client and server will not specify a blocklist or missing blocks.
         * As in ModifyFile, this call should return an error if the server
         * it is called on isn't the leader
         * </pre>
         */
        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            lock0.lock();
            String filename = request.getFilename();
            WriteResult response;

            if (!isLeader) {
                response = WriteResult.newBuilder().setResult(WriteResult.Result.NOT_LEADER).build();
                return;
            }

            // file exists
            else if (fileSystemMap.containsKey(filename)) {
                int storedFileVersion = fileSystemMap.get(filename).getVersion();
                // check file version
                if (request.getVersion() != storedFileVersion + 1) {
                    response = WriteResult.newBuilder().setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(storedFileVersion).build();
                }
                // ready to delete
                else {
                    FileInfo deleteRequest = FileInfo.newBuilder().setFilename(filename).setVersion(storedFileVersion + 1).addBlocklist("0").build();
                    response = commitTransaction(deleteRequest);
                }
            }

            // file does not exist
            else {
                response = WriteResult.newBuilder().setResult(WriteResult.Result.OK).setCurrentVersion(0).build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            lock0.unlock();
        }

        public WriteResult commitTransaction(FileInfo request){
            String filename = request.getFilename();
            WriteResult response;
            int storedFileVersion = 0;

            // File exists on MetadataStore
            if (fileSystemMap.containsKey(filename)) {
                storedFileVersion = fileSystemMap.get(filename).getVersion();
            }

            boolean majority;
            int numNodesAvailable = 0;
            for (MetadataStoreGrpc.MetadataStoreBlockingStub node : nodeList) {
                try {
                    WriteResult nodeResponse = node.prepareTransaction(request);
                    if (nodeResponse.getResult() == WriteResult.Result.OK) {
                        numNodesAvailable++;
                    }
                }
                catch (io.grpc.StatusRuntimeException e) {
                    e.printStackTrace();
                }
                finally {

                }
            }
            majority = numNodesAvailable >= Math.ceil(nodeList.size()/2);
            if (majority) {
                lock1.lock();
                storedFileVersion = 0;
                if (fileSystemMap.containsKey(filename)) {
                    storedFileVersion = fileSystemMap.get(filename).getVersion();
                }
                assert request.getVersion() == storedFileVersion + 1: "Version mismatch";
                // Commit changes to followers
                for (MetadataStoreGrpc.MetadataStoreBlockingStub node : nodeList) {
                    assert node.confirmTransaction(request).getResult() == WriteResult.Result.OK : "Follower fatal error";
                }
                response = WriteResult.newBuilder()
                        .setResult(WriteResult.Result.OK)
                        .setCurrentVersion(request.getVersion())
                        .build();
                // Commit changes locally
                fileSystemMap.put(filename, request);
                confirmedTransactions.add(request);
                lock1.unlock();
            }
            // Not reaching majority
            else {
                response = WriteResult.newBuilder().setResult(WriteResult.Result.MINORITY_COMMIT).setCurrentVersion(storedFileVersion).build();
            }
            return response;
        }
        
        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            assert !isLeader : "Invalid call crash() on leader";
            crashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.isLeader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            assert !isLeader : "Invalid call restore() on leader";
            this.crashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.crashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
   
        }

        /**
         * <pre>
         * Returns the current committed version of the requested file
         * The argument's FileInfo only has the "filename" field defined
         * The FileInfo returns the filename and version fields only
         * This should return a result even if the follower is in a
         *   crashed state
         * </pre>
         */
        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String filename = request.getFilename();
            FileInfo response;
            if (!fileSystemMap.keySet().contains(filename)){
                response = FileInfo.newBuilder().setFilename(filename).setVersion(0).build();
            }
            else {
                int storedFileVersion = fileSystemMap.get(filename).getVersion();
                response = FileInfo.newBuilder().setFilename(filename).setVersion(storedFileVersion).build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void confirmTransaction(surfstore.SurfStoreBasic.FileInfo request,
                                       io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            if (!this.pendingTransactions.isEmpty()){
                FileInfo lastPendingTransaction = pendingTransactions.get(pendingTransactions.size() - 1);
                // ensure last pending request is the same as incoming request
                confirmedTransactions.add(lastPendingTransaction);
                pendingTransactions.remove(lastPendingTransaction);
                assert pendingTransactions.size() == 0 : "Too many pending transactions";
            }
            else {
                System.err.println("pendingTransactions empty when confirming transaction");
            }
        }

        @Override
        public void prepareTransaction(surfstore.SurfStoreBasic.FileInfo request,
                                       io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult response;
            String filename = request.getFilename();
            int storedFileVersion = 0;

            if (this.crashed) {
                response = WriteResult.newBuilder().setResult(WriteResult.Result.CRASHED).build();
            }
            // case when follower node not crashed
            else {
                // File exists on MetadataStore
                if (fileSystemMap.containsKey(filename)) {
                    storedFileVersion = fileSystemMap.get(filename).getVersion();
                }

                // Version mismatch
                if (request.getVersion() <= storedFileVersion) {
                    response = WriteResult.newBuilder().setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(storedFileVersion).build();
                }

                // version OK
                else {
                    response = WriteResult.newBuilder()
                            .setResult(WriteResult.Result.OK)
                            .setCurrentVersion(request.getVersion())
                            .build();
                    this.pendingTransactions.add(request);
                }
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public class AppendLogTimerTask extends TimerTask {
            @Override
            public void run() {
                completeTask();
            }

            private void completeTask() {
                int lastTransactionId = confirmedTransactions.size() - 1;
                if (lastTransactionId < 0) {
                    return;
                }
                FileInfo fileInfo = confirmedTransactions.get(lastTransactionId);
                SurfStoreBasic.LogItem logItem = SurfStoreBasic.LogItem.newBuilder()
                        .setId(lastTransactionId)
                        .setFileInfo(fileInfo)
                        .build();
                SurfStoreBasic.Log log = SurfStoreBasic.Log.newBuilder().addLog(logItem).build();
                // append to all nodes
                for (MetadataStoreGrpc.MetadataStoreBlockingStub node : nodeList) {
                    SurfStoreBasic.Log appendResultLog = node.appendLog(log);
                    // transmit missing logs
                    SurfStoreBasic.Log.Builder appendMissingLogBuilder = SurfStoreBasic.Log.newBuilder();
                    if (appendResultLog.getHasMissingLog()) {
                        for (int i : appendResultLog.getMissingLogIndicesList()){
                            SurfStoreBasic.LogItem appendMissingLogItem = SurfStoreBasic.LogItem.newBuilder()
                                    .setId(i).setFileInfo(confirmedTransactions.get(i)).build();
                            appendMissingLogBuilder.addLog(appendMissingLogItem);
                            assert node.appendLog(appendMissingLogBuilder.build()).getHasMissingLog() == false : "Node " + i + " fatal error";
                        }
                    }
                }
            }
        }

        /**
         * Called on follower only
         * Return true if log matches
         * Return false if more logs are needed
         */
        @Override
        public void appendLog(surfstore.SurfStoreBasic.Log request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Log> responseObserver) {
            List<SurfStoreBasic.LogItem> remoteLogList = request.getLogList();
            boolean hasMissingLog = false;
            for (SurfStoreBasic.LogItem logItem : remoteLogList) {
                int id = logItem.getId();
                while (confirmedTransactions.size() < id) {
                    confirmedTransactions.add(null);
                    hasMissingLog = true;
                }
                confirmedTransactions.add(logItem.getFileInfo());
            }
            SurfStoreBasic.Log.Builder responseBuilder = SurfStoreBasic.Log.newBuilder();
            responseBuilder.setHasMissingLog(hasMissingLog);
            if (hasMissingLog) {
                ArrayList<Integer> missingLogIndices = new ArrayList<>();
                for (int i = 0; i < confirmedTransactions.size(); i++) {
                    if (confirmedTransactions.get(i) == null) {
                        missingLogIndices.add(new Integer(i));
                    }
                }
                SurfStoreBasic.Log.newBuilder().setHasMissingLog(true).addAllMissingLogIndices(missingLogIndices);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(c_args, config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }
}
