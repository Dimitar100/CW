import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Controller {

    private static class Index {
        private static HashMap<String, Integer> stored_files = new HashMap<>();
        static HashMap<String, IndexState> files_states = new HashMap<>();
        static HashMap<Integer, ArrayList<String>> dstores = new HashMap<>();
    }

    private enum IndexState {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    //private static HashSet<Integer> dstores_ports;
    private static HashMap<ServiceThread, String> dstores_connections;
    private static HashMap<Integer, PrintStream> dstores_outs = new HashMap<>();
    private static HashMap<PrintStream, Integer> load_to_client = new HashMap<>();

    private static Integer R = null;
    private static Integer timeout = null;
    private static Integer rebalance_period = null;

    private static ServerSocket ss = null;
    private static HashSet<String> commands_from_client;

    public static void main(String [] args) {

        dstores_connections = new HashMap<>();
        commands_from_client = new HashSet<>();
        String[] commands = {"STORE", "LOAD", "RELOAD", "REMOVE", "LIST"};
        commands_from_client.addAll(Arrays.asList(commands));

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
                while((line = in.readLine()) != null) {

                    String[] split_line = line.split(" ");
                    String command = split_line[0];

                    if(commands_from_client.contains(command)) {
                        System.out.println("Client: "+line);
                        if (Index.dstores.size() < R) {
                            out_to_socket.println("ERROR_NOT_ENOUGH_DSTORES");
                        } else {
                            new Thread(new ClientConnection(out_to_socket, split_line, ss)).start();
                        }
                    }else{
                        System.out.println("Dstore: "+line);
                        if(command.equals("JOIN")) {
                            Index.dstores.put(convertStringToInt(split_line[1]), new ArrayList<>());
                            dstores_connections.put(this, "JOIN");
                            dstores_outs.put(convertStringToInt(split_line[1]), out_to_socket);
                            //rebalance
                        }else {
                            if (command.equals("STORE_ACK") || command.equals("REMOVE_ACK")){
                                dstores_connections.put(this, line);
                            }
                        }
                    }
                }
                socket.close();
            } catch(Exception e) { System.err.println("error: " + e); }
        }
    }

    static class ClientConnection implements Runnable {
        PrintStream out;
        String[] split_line;
        ServerSocket ss;
        String command;
        String filename;
        ClientConnection(PrintStream out, String[] split_line, ServerSocket ss) {
            this.out=out;
            this.split_line = split_line;
            this.ss = ss;
            this.command = split_line[0];
            if(split_line.length > 1){
                filename = split_line[1];
            }
        }
        public void run() {

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
                        int i = 0;
                        for (Integer p : Index.dstores.keySet()) {
                            dports.append(" ").append(p.toString());
                            i++;
                        }
                        //int number_of_dstores = Index.dstores.size();
                        //int number_of_files = Index.stored_files.size();
                        out.println("STORE_TO" + dports);

                        CountDownLatch latch = new CountDownLatch(i);

                        for(ServiceThread dstore_connection : dstores_connections.keySet()) {
                            new Thread(new ACK_Receiver(dstore_connection, "STORE_ACK "+filename, latch)).start();
                        }

                        try {
                            latch.await();
                            Index.files_states.put(filename, IndexState.STORE_COMPLETE);
                            out.println("STORE_COMPLETE"); // to client
                            Index.stored_files.put(filename, Integer.parseInt(split_line[2]));

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case "LOAD": {
                    Integer[] dports = Index.dstores.keySet().toArray(new Integer[0]);

                    if (!Index.files_states.containsKey(filename )|| !Index.stored_files.containsKey(filename)) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                    } else if (Index.files_states.get(filename).equals(IndexState.STORE_IN_PROGRESS)
                            || Index.files_states.get(filename).equals(IndexState.REMOVE_IN_PROGRESS)
                            || Index.files_states.get(filename).equals(IndexState.REMOVE_COMPLETE)){
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                    } else {
                        load_to_client.put(out, 0);
                        out.println("LOAD_FROM " + dports[0] + " " + Index.stored_files.get(filename));
                    }
                    break;
                }
                case "RELOAD": {
                    Integer[] dports = Index.dstores.keySet().toArray(new Integer[0]);
                    int dport_index = load_to_client.get(out) + 1;
                    if (dport_index >= Index.dstores.size()) {
                        out.println("ERROR_LOAD");
                    } else {
                        load_to_client.put(out, dport_index);
                        out.println("LOAD_FROM " + dports[dport_index] + " " + Index.stored_files.get(filename));
                    }
                    break;
                }
                case "REMOVE": {
                    Integer[] dports = Index.dstores.keySet().toArray(new Integer[0]);

                    if (!Index.files_states.containsKey(filename)) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                    } else if (Index.files_states.get(filename).equals(IndexState.STORE_IN_PROGRESS)
                            || Index.files_states.get(filename).equals(IndexState.REMOVE_IN_PROGRESS)
                            || Index.files_states.get(filename).equals(IndexState.REMOVE_COMPLETE)) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                    } else {
                        Index.files_states.put(filename, IndexState.REMOVE_IN_PROGRESS);

                        int i = 0;

                        for (int dport : dports) {
                            dstores_outs.get(dport).println("REMOVE " + filename);
                            i++;
                        }

                        CountDownLatch latch = new CountDownLatch(i);
                        for(ServiceThread dstore_connection : dstores_connections.keySet()) {
                            new Thread(new ACK_Receiver(dstore_connection, "REMOVE_ACK "+filename, latch)).start();
                        }
                        try {
                            latch.await();
                            Index.files_states.put(filename, IndexState.REMOVE_COMPLETE);
                            Index.files_states.remove(filename);
                            Index.stored_files.remove(filename);
                            out.println("REMOVE_COMPLETE"); // to client
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                }
            }
        }
    }

    static class ACK_Receiver implements Runnable {
        ServiceThread dstore_connection;
        String expected_line;
        CountDownLatch latch;

        ACK_Receiver(ServiceThread dstore_connection, String expected_line, CountDownLatch latch){
            this.dstore_connection = dstore_connection;
            this.expected_line = expected_line;
            this.latch = latch;
        }
        public void run() {
            String line;
            while(true) {
                line = dstores_connections.get(dstore_connection);
                if(line.equals(expected_line)){
                    dstores_connections.put(dstore_connection, "");
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
