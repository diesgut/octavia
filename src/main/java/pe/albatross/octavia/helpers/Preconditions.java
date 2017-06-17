package pe.albatross.octavia.helpers;

import pe.albatross.octavia.exceptions.OctaviaException;

public class Preconditions {

    public static void isNull(Object obj, String msg) {
        if (obj != null) {
            throw new OctaviaException(msg);
        }
    }

    public static void isNotNull(Object obj, String msg) {
        isNotNull(obj, msg, true);
    }

    public static void isNotNull(Object obj, String msg, boolean checkString) {
        if (obj == null) {
            throw new OctaviaException(msg);
        }

        if (checkString) {
            if (obj instanceof String) {
                if (((String) obj).trim().equals("")) {
                    throw new OctaviaException(msg);
                }
            }
        }
    }

    public static void isNotBlank(String string, String msg) {
        if (string == null) {
            throw new OctaviaException(msg);
        }
        if (string.trim().equals("")) {
            throw new OctaviaException(msg);
        }
    }

    public static void isBlank(String string, String msg) {
        if (string == null) {
            return;
        }
        if (!string.trim().equals("")) {
            throw new OctaviaException(msg);
        }
    }

    public static void isFalse(Boolean condition, String msg) {
        if (condition) {
            throw new OctaviaException(msg);
        }
    }

    public static void isTrue(Boolean condition, String msg) {
        if (!condition) {
            throw new OctaviaException(msg);
        }
    }

}
