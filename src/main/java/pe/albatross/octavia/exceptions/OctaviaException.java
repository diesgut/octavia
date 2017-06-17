package pe.albatross.octavia.exceptions;

public class OctaviaException extends RuntimeException {

    public OctaviaException() {
    }

    public OctaviaException(String message) {
        super(message);
    }

    public OctaviaException(String message, Object... args) {
        super(String.format(message, args));
    }

}
