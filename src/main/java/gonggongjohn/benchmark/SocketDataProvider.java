package gonggongjohn.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class SocketDataProvider {
    public static void main(String[] args) throws IOException, InterruptedException {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getByName(host));
        Random random = new Random();
        Socket socket = serverSocket.accept();
        OutputStream writer = socket.getOutputStream();
        int event = Integer.parseInt(args[0]);
        int totalTime = Integer.parseInt(args[1]);
        int randomBound = Integer.parseInt(args[2]);
        int period = totalTime / event;
        double nanoPeriod = period * 1e6;
        for(int i = 0; i < event; i++){
            long timestamp = System.currentTimeMillis();
            String message = random.nextInt(randomBound) + "," + timestamp + "\n";
            writer.write(message.getBytes(StandardCharsets.UTF_8));
            writer.flush();
            if(period >= 1) {
                Thread.sleep(totalTime / event);
            }
            else{
                Thread.sleep(0, (int)nanoPeriod);
            }
        }
        socket.close();
        serverSocket.close();
    }
}
