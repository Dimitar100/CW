package mitko.code;

import java.io.*;
import java.net.*;
import java.util.HashSet;

public class Controller {
    private static int index = 0;
    private static HashSet<Integer> dstores;

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
                //Read on cport
                new Thread(new ServiceThread(client, R)).start();
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

        ServiceThread(Socket client, int R) throws IOException {
            this.client = client;
            out_to_client = new PrintStream(client.getOutputStream());
            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            required_dstores = R;
        }
        public void run() {
            try {
                String line;
                while((line = in.readLine()) != null) {

                    System.out.println(line);

                    if(line.contains("JOIN")){
                        dstores.add(convertStringToInt(line.split(" ")[1]));
                    }else{
                        if(dstores.size() < required_dstores){
                            out_to_client.println("ERROR_NOT_ENOUGH_DSTORES");
                        }else{
                            //Connect with client;
                            new Thread(new ClientConnection(out_to_client, line)).start();
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
        ClientConnection(PrintStream out, String s) {
            this.out=out;
            cmd = s;
        }
        public void run() {
            String command = cmd.split(" ")[0];
            if(command.equals("LIST")){
                String[] test = new String[2];
                out.println("LIST test0");

            }else if(command.equals("STORE")){
                // updates index, “store in progress”
                String dports = "";
                for(Integer p : dstores){
                    dports = dports + " " + p.toString();
                }
                out.println("STORE_TO" + dports);
            }else if(command.equals("STORE_ACK")){
                //Once Controller received all acks updates index, “store complete”
                // Защо не бачка //Нова нишка мисля че ще оправи проблема
               // out.println("STORE_COMPLETE");


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
