import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.*;

public class EnterPoint {

    private static final File inputFile = new File("G:\\tmp\\inputTest.txt");
    private static final File outFile = new File("G:\\tmp\\out.txt");
    private static final File errorFile = new File("G:\\tmp\\error.txt");

    public static void main(String[] args) {

        if (inputFile.exists() && inputFile.length() > 0) {
            ActorSystem system = ActorSystem.create("system");

            final ActorRef kernel = system.actorOf(Props.create(Kernel.class, outFile, errorFile), "kernel");

            kernel.tell(inputFile, ActorRef.noSender());
        } else {
            System.err.println("File does not exist or file is empty: " + inputFile.getName());
        }
    }
}
