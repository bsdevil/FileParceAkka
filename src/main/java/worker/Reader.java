package worker;

import akka.actor.UntypedActor;

import java.io.*;

public class Reader extends UntypedActor {

    @Override
    public void onReceive(Object obj) throws Exception {

        if (obj instanceof Job) {
            Job job = (Job) obj;

            BufferedReader reader = new BufferedReader(new FileReader(job.inputFile));
            String tmpStr;
            String key, value;
            String tmpArray[] = new String[2];
//            System.out.println("Start job: " + job.jobId);

            while ((tmpStr = reader.readLine()) != null) {
                tmpArray = tmpStr.split(";");
                try {
                    key = tmpArray[0];
                    value = tmpArray[1];
                    sender().tell(Pair.get(key, value), self());
                } catch (ArrayIndexOutOfBoundsException iobe) {
                    sender().tell(tmpStr, self());
                }
            }
//            System.out.println("Stop job: " + job.jobId);
            sender().tell(job.jobId, self());

        }

        unhandled(obj);
    }

    public static class Job implements Serializable {

        private static final long serialVersionUID = -8058089181084141301L;

        private int jobId;
        private File inputFile;

        public Job(int jobId, File inputFile) {
            this.jobId = jobId;
            this.inputFile = inputFile;
        }
    }

    public static class Pair<A, B> {
        private A key;
        private B value;

        public static <C, D> Pair<C, D> get(C k, D v) {
            Pair<C, D> p = new Pair();
            p.key = k;
            p.value = v;
            return p;
        }

        public A getKey() {
            return key;
        }

        public B getValue() {
            return value;
        }
    }

}
