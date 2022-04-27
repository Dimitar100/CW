package mitko.code;

import java.io.*;
import java.net.*;
import java.util.HashSet;

public class Controller {
    private static int index = 0;
    private static HashSet<Integer> dstores;
    private static PrintStream client_out; // should be hashset
    private static HashSet<String> stored_files = new HashSet<>();

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
                        String file_name = line.split(" ")[1];
                        stored_files.add(file_name);
                        new Thread(new ClientConnection(client_out, "STORE_COMPLETE", ss)).start();
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
            String command = cmd.split(" ")[0];
            if(command.equals("LIST")){
                String files_names = "LIST";
                for(String s : stored_files){
                    files_names = files_names + " " + s;
                }
                out.println(files_names);

            }else if(command.equals("STORE")){
                // updates index, “store in progress”
                String dports = "";
                for(Integer p : dstores){
                    dports = dports + " " + p.toString();
                }
                out.println("STORE_TO" + dports);
            }else if(command.equals("STORE_COMPLETE")){
                //Once Controller received all acks updates index, “store complete”
                out.println("STORE_COMPLETE"); // to client
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
