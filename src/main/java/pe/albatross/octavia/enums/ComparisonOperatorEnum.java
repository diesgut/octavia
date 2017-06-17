package pe.albatross.octavia.enums;

import java.util.HashMap;
import java.util.Map;

public enum ComparisonOperatorEnum {

    EQUAL("="),
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LESS_THAN_EQUALS("<="),
    GREATER_THAN_EQUALS(">="),
    DIFFERENT("<>"),
    DIFFERENT_TWO("!="),
    IN("in"),
    NOT_IN("not in"),
    LIKE("like"),
    NOT_LIKE("not like");

    private final String value;
    private static final Map<String, ComparisonOperatorEnum> lookup = new HashMap<>();

    static {
        for (ComparisonOperatorEnum d : ComparisonOperatorEnum.values()) {
            lookup.put(d.getValue(), d);
        }
    }

    private ComparisonOperatorEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ComparisonOperatorEnum get(String abbreviation) {
        return lookup.get(abbreviation);
    }

    public static String getNombre(String nombre) {

        for (ComparisonOperatorEnum d : ComparisonOperatorEnum.values()) {
            if (d.name().equalsIgnoreCase(nombre)) {
                return d.getValue();
            }
        }

        return nombre;
    }

    public static boolean existValue(String looked) {
        boolean exist = false;

        for (ComparisonOperatorEnum e : ComparisonOperatorEnum.values()) {
            if (e.name().equals(looked)) {
                exist = true;
                break;
            }
        }

        return exist;
    }
}
