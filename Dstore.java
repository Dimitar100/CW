package mitko.code;

import java.io.*;
import java.net.*;

public class Dstore {

    public static void main(String [] args) {
        Socket socket = null;
        ServerSocket ss;
        PrintWriter out_to_controller;

        int port = convertStringToInt(args[0]);
        int cport = convertStringToInt(args[1]);
        int timeout = convertStringToInt(args[2]);
        String file_folder = args[3];

        try {
            //send msg to Controller
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, cport);
            out_to_controller = new PrintWriter(socket.getOutputStream(), true);
            out_to_controller.println("JOIN " + port);
            ////
            //listen on port
            ss = new ServerSocket(port);
            BufferedReader in = null;
            PrintStream out_to_client = null;

            while(true) {
                Socket client = ss.accept();
                try {
                    in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    out_to_client = new PrintStream(client.getOutputStream());
                } catch(Exception e) { System.err.println("error: " + e); }

                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line); //print the received command

                    new Thread(new ClientConnection(out_to_client, out_to_controller, line)).start();
                }
                client.close();

            }


        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (socket != null)
                try { socket.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }

    static class ClientConnection implements Runnable {
        PrintStream out_to_client;
        String cmd;
        PrintWriter out_to_controller;
        ClientConnection(PrintStream out_to_client,  PrintWriter out_to_controller, String cmd) {
            this.out_to_client = out_to_client;
            this.out_to_controller = out_to_controller;
            this.cmd = cmd;
        }
        public void run() {
            try {
                readCommands(cmd, out_to_client, out_to_controller);
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
            //System.out.println("LIST test1 test2 test3");
        }
        private static void readCommands(String cmd, PrintStream out, PrintWriter out_to_controller){
            String command = cmd.split(" ")[0];
            if(command.equals("LOAD")){

            }else if(command.equals("REMOVE")){

            }else if(command.equals("STORE")){
                out.println("ACK");
                //store file content
                //Once the Dstore finishes storing the file
                //out.close();
                new Thread(new ControllerConnection(out_to_controller, cmd.split(" ")[1])).start();
            }
        }
    }

    static class ControllerConnection implements Runnable {
        PrintWriter out;
        String fileName;
        ControllerConnection(PrintWriter out, String s) {
            fileName = s;
            this.out = out;
        }
        public void run() {
            try {
                sendSTORE_ACK(fileName, out);
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }
        private static void sendSTORE_ACK(String filename, PrintWriter out){
            out.println("STORE_ACK " + filename);
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
