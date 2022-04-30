package mitko.code;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

public class Dstore {

    private static HashSet<File> stored_files = new HashSet<>();
    private static File stored_file;
    private static String file_folder;

    public static void main(String [] args) {
        Socket socket_controller;
        ServerSocket ss = null;
        PrintWriter out_to_controller;

        int port = convertStringToInt(args[0]);
        int cport = convertStringToInt(args[1]);
        int timeout = convertStringToInt(args[2]);
        file_folder = args[3];

        try {
            InetAddress address = InetAddress.getLocalHost();
            socket_controller = new Socket(address, cport);
            out_to_controller = new PrintWriter(socket_controller.getOutputStream(), true);
            out_to_controller.println("JOIN " + port);
            
            ss = new ServerSocket(port);

            while(true) {
                Socket socket = ss.accept();
                new Thread(new ClientConnection(socket, out_to_controller)).start();
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try { ss.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }

    static class ClientConnection implements Runnable {
        OutputStream outputStream;
        PrintStream out_to_client;
        PrintWriter out_to_controller;
        BufferedReader in;
        Socket socket;

        ClientConnection(Socket socket, PrintWriter out_to_controller) {
            this.socket = socket;
            this.out_to_controller = out_to_controller;

            try {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out_to_client = new PrintStream(socket.getOutputStream());
                outputStream = socket.getOutputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        public void run() {
            try {

                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line); //print the received command
                    readCommands(line, outputStream, out_to_client, out_to_controller);
                }
                socket.close();
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }
        private static void readCommands(String cmd, OutputStream outputStream, PrintStream out, PrintWriter out_to_controller) {
            String[] command = cmd.split(" ");
            switch (command[0]) {
                case "REMOVE":
                    String removed_file_name = command[1];
                    File file = new File(file_folder + removed_file_name);

                    if (file.delete()) {
                        stored_files.remove(file);
                        new Thread(new ControllerConnection(out_to_controller, command[0], removed_file_name)).start();
                    }

                    break;
                case "STORE":
                    if (!Files.isDirectory(Paths.get(file_folder))) {
                        File storage_folder = new File(file_folder);
                        boolean folder_created = storage_folder.mkdir();
                        if (folder_created) {
                            createFile(command[1]);
                        }
                    } else {
                        createFile(command[1]);
                    }
                    out.println("ACK");
                    break;
                case "LOAD_DATA":
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
                    break;
                default:
                    //store and send ACK to controler;
                    Path fileName = Path.of(stored_file.getPath());
                    try {
                        Files.writeString(fileName, cmd);
                        // Dont write string write data
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    stored_files.add(stored_file);
                    new Thread(new ControllerConnection(out_to_controller, command[0], stored_file.getName())).start();
                    break;
            }
        }

        private static void createFile(String fileName){
            File file = new File(file_folder + fileName);
            boolean result = false;
            try {
                result = file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(result){
                stored_file = file;
            }
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
