package storm.dataclean.exceptions;

/**
 * Created by tian on 26/11/2015.
 */
public class BufferedTupleNotFoundException extends BleachException{

    public static String header = "Buffered tuple not found, id: ";

    public BufferedTupleNotFoundException(String message) {
        super(message);
    }

    public BufferedTupleNotFoundException(int tid) {
        super(header + tid);
    }
}
