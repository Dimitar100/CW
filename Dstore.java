import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;

public class Dstore {

    private static HashSet<File> stored_files = new HashSet<>();
    private static String file_folder;
    //private static ServiceThread controller_connection;

    public static void main(String [] args) {
        Socket socket_controller;
        ServerSocket ss = null;
        PrintWriter out_to_controller;


        int port = convertStringToInt(args[0]);
        int cport = convertStringToInt(args[1]);
        int timeout = convertStringToInt(args[2]);
        file_folder = args[3]+"/";

        File storage_folder = new File(file_folder);
        if (!Files.isDirectory(Paths.get(file_folder))) {
            boolean folder_created = storage_folder.mkdir();
        } else {
            File[] filesList = storage_folder.listFiles();
            assert filesList != null;
            for(File file : filesList) {
                if(file.isFile()) {
                    file.delete();
                }
            }
        }
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket_controller = new Socket(address, cport);
            out_to_controller = new PrintWriter(socket_controller.getOutputStream(), true);
            out_to_controller.println("JOIN " + port);
            //start controller connection;
            ss = new ServerSocket(port);

            while(true) {
                Socket socket = ss.accept();
                new Thread(new ServiceThread(socket, out_to_controller)).start();
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try { ss.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }

    static class ServiceThread implements Runnable {
        OutputStream outputStream;
        PrintStream out_to_client;
        PrintWriter out_to_controller;
        BufferedReader in;
        InputStream inputStream;
        Socket socket;

        File file_to_write;
        OutputStream file_output = null;


        ServiceThread(Socket socket, PrintWriter out_to_controller) {
            this.socket = socket;
            this.out_to_controller = out_to_controller;
            try {
                inputStream = socket.getInputStream();
                in = new BufferedReader(new InputStreamReader(inputStream));
                outputStream = socket.getOutputStream();
                out_to_client = new PrintStream(outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        public void run() {
            try {
                String line;
                byte[] buffer = new byte[1024];
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    if(file_output != null){
                        file_output.write(buffer, 0, bytesRead);
                    }else{
                        byte[] temp = new byte[bytesRead-2];
                        for(int i = 0; i<temp.length; i++){
                            temp[i] = buffer[i];
                        }
                        line = new String(temp);
                        System.out.println(line); //print the received command
                        readCommands(line, outputStream, out_to_client, out_to_controller);
                    }
                }
                if (file_output != null){
                    file_output.close();
                    stored_files.add(file_to_write);
                    file_output = null;
                    new Thread(new ControllerConnection(out_to_controller, "STORE_ACK", file_to_write.getName())).start();
                }
                socket.close();
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }
        private void readCommands(String cmd, OutputStream outputStream, PrintStream out, PrintWriter out_to_controller) {
            String[] command = cmd.split(" ");
            switch (command[0]) {
                case "REMOVE":
                    String removed_file_name = command[1];
                    File file = new File(file_folder + removed_file_name);
                    if(file.exists()){
                        file.delete();
                        stored_files.remove(file);
                        new Thread(new ControllerConnection(out_to_controller, command[0], removed_file_name)).start();
                    }else{
                        new Thread(new ControllerConnection(out_to_controller, "ERROR_FILE_DOES_NOT_EXIST", removed_file_name)).start();
                    }
                    break;
                case "STORE":
                    //createFile(command[1]);
                    file_to_write = new File(file_folder + command[1]);
                    boolean result;
                    try {
                        if(file_to_write.exists()){
                            file_output = new FileOutputStream(file_to_write);
                            new Thread(new ClientConnection(out, command[0])).start();
                        }else{
                            result = file_to_write.createNewFile();
                            if(result){
                                file_output = new FileOutputStream(file_to_write);
                                new Thread(new ClientConnection(out, command[0])).start();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "LOAD_DATA":
                    new Thread(new ClientConnection(outputStream, command[0], command[1])).start();
                    break;
                default:
                    break;
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
                    out.println("REMOVE_ACK " + fileName);
                }else if(command.equals("ERROR_FILE_DOES_NOT_EXIST")){
                    out.println(command);
                }else{
                    out.println("STORE_ACK " + fileName);
                }
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }
    }

    static class ClientConnection implements Runnable {
        PrintStream out;
        String command;
        OutputStream outputStream;
        String filename = null;
        ClientConnection(PrintStream out, String command) {
            this.command = command;
            this.out = out;
        }
        ClientConnection(OutputStream outputStream, String command, String filename) {
            this.command = command;
            this.outputStream = outputStream;
            this.filename = filename;
        }
        public void run() {
            try {
                if(command.equals("STORE")){
                    out.println("ACK");
                }else if(command.equals("LOAD_DATA")){
                    BufferedReader in = new BufferedReader(new FileReader(file_folder + filename));
                    String str;
                    while ((str = in.readLine()) != null) {
                        outputStream.write(str.getBytes());// new thread
                    }
                    in.close();
                }
            } catch(Exception e) {
                System.err.println("error: " + e);
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
