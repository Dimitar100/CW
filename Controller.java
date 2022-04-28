package mitko.code;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;

public class Controller {
    private static HashSet<Integer> dstores;
    private static PrintStream client_out; // should be hashset
    private static HashMap<String, Integer> stored_files = new HashMap<>();
    private static HashMap<String, Integer> ACKs = new HashMap<>();
    private static HashMap<PrintStream, Integer> load_to_client = new HashMap<>();

    public static void main(String [] args) {
        ServerSocket ss = null;
        dstores = new HashSet<Integer>();
        try {
            int port = convertStringToInt(args[0]);
            int R = convertStringToInt(args[1]);
            int timeout = convertStringToInt(args[2]);
            int rebalance_period = convertStringToInt(args[3]);

            ss = new ServerSocket(port);

            while (true) {
                Socket client = ss.accept(); // accept connections
               // System.out.println(client.connect);
                //Read on cport
                new Thread(new ServiceThread(client, R, ss)).start();
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try { ss.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }

    static class ServiceThread implements Runnable {
        BufferedReader in;
        PrintStream out_to_client;
        Socket client;
        int required_dstores;
        ServerSocket ss;

        ServiceThread(Socket client, int R, ServerSocket ss) throws IOException {
            this.client = client;
            out_to_client = new PrintStream(client.getOutputStream());
            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            required_dstores = R;
            this.ss = ss;
        }
        public void run() {
            try {
                String line;
                while((line = in.readLine()) != null) {

                    System.out.println(line);

                    if(line.contains("JOIN")) {
                        dstores.add(convertStringToInt(line.split(" ")[1]));
                    }else if(line.contains("STORE_ACK")){
                        String filename = line.split(" ")[1];
                        int updated_counter = ACKs.get(filename)-1;
                        ACKs.put(filename, updated_counter);
                        if(ACKs.get(filename) == 0){
                            new Thread(new ClientConnection(client_out, line, ss)).start();
                        }

                    }else if(line.contains("REMOVE_ACK")){
                        //temp
                        String filename = line.split(" ")[1];
                        int updated_counter = ACKs.get(filename)-1;
                        ACKs.put(filename, updated_counter);
                        if(ACKs.get(filename) == 0){
                            new Thread(new ClientConnection(client_out, line, ss)).start();
                        }
                    }else{
                        if(dstores.size() < required_dstores){
                            out_to_client.println("ERROR_NOT_ENOUGH_DSTORES");
                        }else{
                            //Connect with client;
                            client_out =out_to_client; // save printstream to client
                            new Thread(new ClientConnection(out_to_client, line, ss)).start();
                            //
                        }
                    }
                }
                client.close();
            } catch(Exception e) { System.err.println("error: " + e); }
        }
    }

    static class ClientConnection implements Runnable {
        PrintStream out;
        String cmd;
        ServerSocket ss;
        ClientConnection(PrintStream out, String s, ServerSocket ss) {
            this.out=out;
            cmd = s;
            this.ss = ss;
        }
        public void run() {
            String command[] = cmd.split(" ");
            if(command[0].equals("LIST")){
                String files_names = "LIST";
                for(String s : stored_files.keySet()){
                    files_names = files_names + " " + s;
                }
                out.println(files_names);

            }else if(command[0].equals("STORE")){
                // updates index, “store in progress”
                if(Index.files_states.containsKey(command[1]) && Index.files_states.get(command[1]) == IndexState.STORE_IN_PROGRESS){
                    out.println("ERROR_FILE_ALREADY_EXISTS");
                }else{
                    Index.files_states.put(command[1], IndexState.STORE_IN_PROGRESS);
                    Index.stored_files.put(command[1], Integer.parseInt(command[2]));
                    String dports = "";
                    int i = 0;
                    for(Integer p : dstores){
                        dports = dports + " " + p.toString();
                        i++;
                    }
                    ACKs.put(command[1], i);
                    out.println("STORE_TO" + dports);
                }
            }else if(command[0].equals("STORE_ACK")){
                //Once Controller received all acks updates index, “store complete”
                stored_files.put(command[1], Index.stored_files.get(command[1]));
                Index.files_states.put(command[1], IndexState.STORE_COMPLETE);
                out.println("STORE_COMPLETE"); // to client
            }else if(command[0].equals("LOAD")){
                Integer[] dports = dstores.toArray(new Integer[0]);

                if(!Index.files_states.containsKey(command[1])){
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                }else if( Index.files_states.get(command[1]).equals(IndexState.STORE_IN_PROGRESS)
                        || Index.files_states.get(command[1]).equals(IndexState.REMOVE_IN_PROGRESS) ){
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                }else{
                    load_to_client.put(out, 0);
                    out.println("LOAD_FROM " + dports[0]+ " " + stored_files.get(command[1]));
                }
            }else if(command[0].equals("RELOAD")){
                Integer[] dports = dstores.toArray(new Integer[0]);
                int dport_index = load_to_client.get(out)+1;
                if(dport_index >= dstores.size()){
                    out.println("ERROR_LOAD");
                }else {
                    load_to_client.put(out, dport_index);
                    out.println("LOAD_FROM " + dports[dport_index] + " " + stored_files.get(command[1]));
                }
            }else if(command[0].equals("REMOVE")){
                //Controller updates index, “remove in progress”

                Integer[] dports = dstores.toArray(new Integer[0]);
                InetAddress address = null;
                Socket socket = null;
                PrintWriter out_to_dport = null;
                if(!Index.files_states.containsKey(command[1])){
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                }else if(Index.files_states.get(command[1]).equals(IndexState.STORE_IN_PROGRESS)
                        || Index.files_states.get(command[1]).equals(IndexState.REMOVE_IN_PROGRESS)){
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                }else {
                    Index.files_states.put(command[1], IndexState.REMOVE_IN_PROGRESS);

                    try {
                        int i = 0;
                        address = InetAddress.getLocalHost();
                        for (int dport : dports) {
                            socket = new Socket(address, dport);
                            out_to_dport = new PrintWriter(socket.getOutputStream(), true);
                            out_to_dport.println("REMOVE " + command[1]);
                            i++;
                        }
                        ACKs.put(command[1], i);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }else if(command[0].equals("REMOVE_ACK")){
                //update index
                Index.files_states.put(command[1], IndexState.REMOVE_COMPLETE);
                stored_files.remove(command[1]);
                out.println("REMOVE_COMPLETE"); // to client
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
