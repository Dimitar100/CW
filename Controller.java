import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {

    private static class Index {
        private static HashMap<String, Integer> stored_files = new HashMap<>();
        static HashMap<String, IndexState> files_states = new HashMap<>();
       // static HashMap<Integer, ArrayList<String>> dstores = new HashMap<>();
        static HashMap<String, ArrayList<Integer>> dstores_storing_file = new HashMap<>();
    }

    private enum IndexState {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    private static HashSet<Integer> dstores = new HashSet<>();
    private static HashMap<Integer, String> dstores_connections = new HashMap<>();
    private static HashMap<Integer, PrintStream> dstores_outs = new HashMap<>();
    private static HashMap<PrintStream, Integer> load_to_client = new HashMap<>();

    private static Integer R = null;
    private static Integer timeout = null;
    private static Integer rebalance_period = null;

    private static ServerSocket ss = null;

    public static void main(String [] args) {

        try {
            int port = convertStringToInt(args[0]);
            R = convertStringToInt(args[1]);
            timeout = convertStringToInt(args[2]);
            rebalance_period = convertStringToInt(args[3]);

            ss = new ServerSocket(port);

            while (true) {
                Socket client = ss.accept(); // accept connections
                //Read on cport
                new Thread(new ServiceThread(client)).start();
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try { ss.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }

    static class ServiceThread implements Runnable {
        BufferedReader in;
        PrintStream out_to_socket;
        Socket socket;

        ServiceThread(Socket socket) throws IOException {
            this.socket = socket;
            out_to_socket = new PrintStream(socket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }
        public void run() {
            try {
                String line;
                line = in.readLine();
                String[] split_line = line.split(" ");
                String command = split_line[0];

                if(command.equals("JOIN")) {
                    System.out.println("Dstore: "+line);
                    int dport = convertStringToInt(split_line[1]);
                    dstores.add(dport);
                    dstores_outs.put(dport, out_to_socket);
                    new Thread(new ReadFromDstore(out_to_socket, in, ss, socket, dport)).start();
                    //rebalance
                } else {
                    System.out.println("Client: "+line);
                    new Thread(new HandleClientCommands(out_to_socket, split_line, ss)).start();
                    new Thread(new ReadFromClient(out_to_socket, in, ss, socket)).start();
                }
            } catch(Exception e) { System.err.println("error: " + e); }
        }
    }

    static class ReadFromClient implements Runnable{
        PrintStream out;
        BufferedReader in;
        ServerSocket ss;
        Socket socket;

        ReadFromClient(PrintStream out, BufferedReader in, ServerSocket ss, Socket socket) {
            this.out = out;
            this.in = in;
            this.ss = ss;
            this.socket = socket;
        }

        @Override
        public void run() {
            String line;
            try {
                while((line = in.readLine()) != null) {
                    String[] split_line = line.split(" ");
                    System.out.println("Client: "+line);
                    new Thread(new HandleClientCommands(out, split_line, ss)).start();
                }
                socket.close();
            } catch(Exception e) { System.err.println("error: " + e); }
        }
    }

    static class HandleClientCommands implements Runnable {
        PrintStream out;
        String[] split_line;
        ServerSocket ss;
        String command;
        String filename;

        HandleClientCommands(PrintStream out, String[] split_line, ServerSocket ss) {
            this.out=out;
            this.split_line = split_line;
            this.ss = ss;
            this.command = split_line[0];
            if(split_line.length > 1){
                filename = split_line[1];
            }
        }
        public void run() {
            if (dstores.size() < R) {
                out.println("ERROR_NOT_ENOUGH_DSTORES");
            } else {
                switch (command) {
                    case "LIST":
                        StringBuilder files_names = new StringBuilder("LIST");
                        for (String s : Index.stored_files.keySet()) {
                            files_names.append(" ").append(s);
                        }
                        out.println(files_names);
                        break;
                    case "STORE":
                        if (Index.stored_files.containsKey(filename)
                                || Index.files_states.get(filename) == IndexState.STORE_IN_PROGRESS
                                || Index.files_states.containsKey(filename)) {
                            out.println("ERROR_FILE_ALREADY_EXISTS");
                        } else {
                            Index.files_states.put(filename, IndexState.STORE_IN_PROGRESS);

                            StringBuilder dports = new StringBuilder();
                            ArrayList<Integer> dstores_ports = new ArrayList<>();
                            int i;
                            for (i = 0; i < R; i++) {
                                Integer p = (Integer) dstores.toArray()[i];
                                dstores_ports.add(p);
                                dports.append(" ").append(p.toString());
                            }
                            //int number_of_dstores = Index.dstores.size();
                            //int number_of_files = Index.stored_files.size();
                            out.println("STORE_TO" + dports);
                            CountDownLatch latch = new CountDownLatch(R);//not sure

                            for (Integer dport : dstores_ports) {
                                new Thread(new ACK_Receiver(dport, "STORE_ACK " + filename, latch)).start();
                            }

                            try {
                                boolean timer = latch.await(timeout, TimeUnit.MILLISECONDS);
                                if (timer) {
                                    Index.files_states.put(filename, IndexState.STORE_COMPLETE);
                                    Index.stored_files.put(filename, Integer.parseInt(split_line[2]));
                                    Index.dstores_storing_file.put(filename, dstores_ports);
                                    out.println("STORE_COMPLETE"); // to client
                                } else {
                                    Index.files_states.remove(filename);
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    case "LOAD": {
                        ArrayList<Integer> dports = Index.dstores_storing_file.get(filename);

                        if (!Index.files_states.containsKey(filename) || !Index.stored_files.containsKey(filename)) {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                        } else if (Index.files_states.get(filename).equals(IndexState.STORE_IN_PROGRESS)
                                || Index.files_states.get(filename).equals(IndexState.REMOVE_IN_PROGRESS)
                                || Index.files_states.get(filename).equals(IndexState.REMOVE_COMPLETE)) {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                        } else {
                            load_to_client.put(out, 0);
                            out.println("LOAD_FROM " + dports.get(0) + " " + Index.stored_files.get(filename));
                        }
                        break;
                    }
                    case "RELOAD": {
                        ArrayList<Integer> dports = Index.dstores_storing_file.get(filename);
                        int dport_index = load_to_client.get(out) + 1;
                        if (dport_index >= dports.size()) {
                            out.println("ERROR_LOAD");
                        } else {
                            load_to_client.put(out, dport_index);
                            out.println("LOAD_FROM " + dports.get(dport_index) + " " + Index.stored_files.get(filename));
                        }
                        break;
                    }
                    case "REMOVE": {
                        ArrayList<Integer> dports = Index.dstores_storing_file.get(filename);

                        if (!Index.files_states.containsKey(filename)) {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                        } else if (Index.files_states.get(filename).equals(IndexState.STORE_IN_PROGRESS)
                                || Index.files_states.get(filename).equals(IndexState.REMOVE_IN_PROGRESS)
                                || Index.files_states.get(filename).equals(IndexState.REMOVE_COMPLETE)) {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                        } else {
                            Index.files_states.put(filename, IndexState.REMOVE_IN_PROGRESS);
                            CountDownLatch latch = new CountDownLatch(dports.size());
                            for (Integer dport : dports) {
                                dstores_outs.get(dport).println("REMOVE " + filename);
                                new Thread(new ACK_Receiver(dport, "REMOVE_ACK " + filename, latch)).start();
                            }
                            try {
                                latch.await(timeout, TimeUnit.MILLISECONDS);
                                Index.files_states.put(filename, IndexState.REMOVE_COMPLETE);
                                Index.files_states.remove(filename);
                                Index.stored_files.remove(filename);
                                Index.dstores_storing_file.remove(filename);
                                out.println("REMOVE_COMPLETE");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    static class ReadFromDstore implements Runnable{
        PrintStream out;
        BufferedReader in;
        ServerSocket ss;
        Socket socket;
        Integer port;

        ReadFromDstore(PrintStream out, BufferedReader in, ServerSocket ss, Socket socket, Integer port) {
            this.out = out;
            this.in = in;
            this.ss = ss;
            this.socket = socket;
            this.port = port;
        }

        @Override
        public void run() {
            String line;
            try {
                dstores_connections.put(port, "JOIN");
                while((line = in.readLine()) != null) {
                   // String[] split_line = line.split(" ");
                    System.out.println("Dstore: "+line);
                    dstores_connections.put(port, line);
                }
                socket.close();
            } catch(Exception e) {
                //System.err.println("error: " + e);
                System.out.println("Dstore " + port + ": " + "disconnected");
                dstores.remove(port);
            }
        }
    }

    static class ACK_Receiver implements Runnable {
        Integer dport;
        String expected_line;
        CountDownLatch latch;

        ACK_Receiver(Integer dport, String expected_line, CountDownLatch latch){
            this.dport = dport;
            this.expected_line = expected_line;
            this.latch = latch;
        }
        public void run() {
            String line;
            while(true) {
                line = dstores_connections.get(dport);
                if(line.equals(expected_line)){
                    dstores_connections.put(dport, "");
                    latch.countDown();
                    break;
                }
            }
        }
    }

    private static int convertStringToInt(String str) {
        int val = 0;
        try {
            val = Integer.parseInt(str);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid String");
        }
        return val;
    }
}
