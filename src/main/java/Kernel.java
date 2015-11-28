
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import worker.Parser;
import worker.Reader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Kernel extends UntypedActor {

    private File outFile;
    private File errorFile;
    private ActorRef routerReaders;
    private Long workTime;


    private Map<String, ActorRef> parserMap = new HashMap<String, ActorRef>();
    private List<String> resultList = new ArrayList<String>();
    private List<String> errorList = new ArrayList<String>();
    private Set<Integer> jobs = new HashSet<Integer>();
    private int jobIdCounter = 0;


    public Kernel(File outFile, File errorFile) {
        this.outFile = outFile;
        this.errorFile = errorFile;
        workTime = System.currentTimeMillis();
    }

    @Override
    public void onReceive(Object obj) throws Exception {
        if (obj instanceof File) {// start file reading
            routerReaders = getContext().actorOf(Props.create(Reader.class), "reader");
            routerReaders.tell(new Reader.Job(jobIdCounter, (File) obj), self());
            jobs.add(jobIdCounter++);

        } else if (obj instanceof Reader.Pair) { //resend to parser
            //проще и быстрее сразу обработать результат, но сделаем через акторы, каждый будет обрабатывать свой id
            ActorRef parser;
            String key = (String) ((Reader.Pair) obj).getKey();
            if ((parser = parserMap.get(key)) == null) {
                parser = getContext().actorOf(Props.create(Parser.class, key));
                parserMap.put(key, parser);
            }
            parser.tell(obj, getSelf());

        } else if (obj instanceof Integer) {//end file
            jobs.remove(obj);
            if (jobs.isEmpty()) {
                for (ActorRef ar : parserMap.values()) {
                    ar.tell("END", self());
                }
            }
        } else if (obj instanceof String) { //add error in error list
            errorList.add((String) obj);

        } else if (obj instanceof Parser.ParseResult) { //save result on end parser work
            String id = ((Parser.ParseResult) obj).getId();
            resultList.add(id + ";" + ((Parser.ParseResult) obj).getAmount());
            errorList.addAll(((Parser.ParseResult) obj).getErrorList());

            parserMap.remove(id);
            if (parserMap.isEmpty()) {
                saveResult(outFile, resultList);
                saveResult(errorFile, errorList);
                System.out.println("Work time: " + (System.currentTimeMillis() - workTime) + " ms");
                this.context().system().shutdown();
            }
        }

        unhandled(obj);
    }

    private void saveResult(File outFile, List<String> listToSave) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(outFile));
            for (String row : listToSave) {
                bw.write(row + "\r\n");
            }
            bw.close();
        } catch (IOException e) {
            System.err.println("Can`t create file: " + outFile.getName());
        }

    }

}
