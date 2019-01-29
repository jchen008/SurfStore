package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.SimpleAnswer;


public final class BlockStore {
    private static final Logger logger = Logger.getLogger(BlockStore.class.getName());

    protected Server server;
	protected ConfigReader config;


    public BlockStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new BlockStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockStore.this.stop();
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
        ArgumentParser parser = ArgumentParsers.newFor("BlockStore").build()
                .description("BlockStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
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

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        final BlockStore server = new BlockStore(config);
        server.start(config.getBlockPort(), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class BlockStoreImpl extends BlockStoreGrpc.BlockStoreImplBase {

        protected HashMap<String, byte[]> hashMap;

        public BlockStoreImpl(){
            super();
            this.hashMap = new HashMap<String, byte[]>();
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void storeBlock(surfstore.SurfStoreBasic.Block request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver){
            byte[] B = request.getData().toByteArray();
            String hash = request.getHash();
            hashMap.put(hash,B);
            
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getBlock(surfstore.SurfStoreBasic.Block request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Block> responseObserver){
            
            byte[] data = hashMap.get(request.getHash());
            Block.Builder builder = Block.newBuilder();

            builder.setData(ByteString.copyFrom(data));
            builder.setHash(request.getHash());

            Block B = builder.build();

            responseObserver.onNext(B);
            responseObserver.onCompleted();
        }
            
        @Override
        public void hasBlock(surfstore.SurfStoreBasic.Block request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver){
            boolean answer = hashMap.containsKey(request.getHash());
            
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(answer).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
