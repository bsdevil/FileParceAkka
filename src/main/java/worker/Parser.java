package worker;

import akka.actor.UntypedActor;

import java.util.ArrayList;
import java.util.List;

public class Parser extends UntypedActor {
    private ParseResult parseResult;

    public Parser(String id) {
        parseResult = new ParseResult(id, 0.0, new ArrayList<String>());
    }

    @Override
    public void onReceive(Object obj) throws Exception {
        if (obj instanceof Reader.Pair) {
            try {
                parseResult.amount += Double.valueOf((String) ((Reader.Pair) obj).getValue());
            } catch (NumberFormatException nfe) {
                parseResult.errorList.add(parseResult.id + ";" + ((Reader.Pair) obj).getValue());
            }
        } else if ((obj instanceof String) && (obj.equals("END"))) {
            sender().tell(parseResult, self());
        }
        unhandled(obj);
    }

    public static class ParseResult {
        private String id;
        private Double amount;
        private List<String> errorList;

        private ParseResult(String id, Double amount, List<String> errorList) {
            this.id = id;
            this.amount = amount;
            this.errorList = errorList;
        }

        public String getId() {
            return id;
        }

        public Double getAmount() {
            return amount;
        }

        public List<String> getErrorList() {
            return errorList;
        }
    }
}
