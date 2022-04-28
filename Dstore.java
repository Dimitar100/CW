package mitko.code;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class Dstore {

    //private static HashMap<String, Integer> stored_files = new HashMap<>(); //Hash set of files
    private static HashSet<File> stored_files = new HashSet<>();
    private static File stored_file;
   // private static String stored_file_name;
    //private static int stored_file_size;
    private static String file_folder;

    public static void main(String [] args) {
        Socket socket = null;
        ServerSocket ss;
        PrintWriter out_to_controller;

        int port = convertStringToInt(args[0]);
        int cport = convertStringToInt(args[1]);
        int timeout = convertStringToInt(args[2]);
        file_folder = args[3];

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
            OutputStream clientOutputStream = null;

            while(true) {
                Socket client = ss.accept();
                try {
                    in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    out_to_client = new PrintStream(client.getOutputStream());
                    clientOutputStream = client.getOutputStream();
                } catch(Exception e) { System.err.println("error: " + e); }

                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line); //print the received command

                    new Thread(new ClientConnection(clientOutputStream, out_to_client, out_to_controller, line)).start();
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
        OutputStream outputStream;
        PrintStream out_to_client;
        PrintWriter out_to_controller;
        String cmd;
        ClientConnection(OutputStream outputStream, PrintStream out_to_client,  PrintWriter out_to_controller, String cmd) {
            this.outputStream = outputStream;
            this.out_to_client = out_to_client;
            this.out_to_controller = out_to_controller;
            this.cmd = cmd;
        }
        public void run() {
            try {
                readCommands(cmd, outputStream, out_to_client, out_to_controller);
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }
        private static void readCommands(String cmd, OutputStream outputStream, PrintStream out, PrintWriter out_to_controller) {
            String[] command = cmd.split(" ");
            if (command[0].equals("REMOVE")) {
                String removed_file_name = command[1];
                File file = new File(file_folder + removed_file_name);

                if(file.delete()){
                    new Thread(new ControllerConnection(out_to_controller, command[0], removed_file_name)).start();
                }

            } else if (command[0].equals("STORE")) {
                //store file content
                if (!Files.isDirectory(Paths.get(file_folder))) {
                    File storage_folder = new File(file_folder);
                    boolean folder_created = storage_folder.mkdir();
                    if (folder_created) {
                        createFile(command[1], Integer.parseInt(command[2]));
                    }
                } else {
                    createFile(command[1], Integer.parseInt(command[2]));
                }
                out.println("ACK");
            } else if(command[0].equals("LOAD_DATA")){
                //If Dstore does not have the requested file
                try {
                    BufferedReader in = new BufferedReader(new FileReader(file_folder + stored_file.getName()));
                    String str;

                    while ((str = in.readLine()) != null) {
                        //System.out.println(str);
                        outputStream.write(str.getBytes());
                    }
                    in.close();
                } catch (IOException ignored) {
                }
            }else{
                Path fileName = Path.of(stored_file.getPath());
                try {
                    Files.writeString(fileName, cmd);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //stored_files.put(stored_file_name, stored_file_size);
                stored_files.add(stored_file);
                new Thread(new ControllerConnection(out_to_controller, command[0], stored_file.getName())).start();
            }
        }

        private static void createFile(String fileName, Integer fileSize){
            File file = new File(file_folder + fileName);
            boolean result = false;
            try {
                result = file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            stored_file = file;
        }
    }

    static class ControllerConnection implements Runnable {
        PrintWriter out;
        String fileName;
        String command;
        ControllerConnection(PrintWriter out,String command, String s) {
            fileName = s;
            this.command = command;
            this.out = out;
        }
        public void run() {
            try {
                if(command.equals("REMOVE")){
                    sendREMOVE_ACK(fileName, out);
                }else{
                    sendSTORE_ACK(fileName, out);
                }
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }
        private void sendSTORE_ACK(String filename, PrintWriter out) {
            out.println("STORE_ACK " + filename);
        }
        private void sendREMOVE_ACK(String filename, PrintWriter out) {
            out.println("REMOVE_ACK " + filename);
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
