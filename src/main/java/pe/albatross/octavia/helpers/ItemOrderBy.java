package pe.albatross.octavia.helpers;

public class ItemOrderBy {

    private final OrderTypeEnum orderType;
    private final String column;

    public enum OrderTypeEnum {
        ORDER_ASC, ORDER_DESC
    };

    public ItemOrderBy(String column, OrderTypeEnum typeEnum) {
        this.orderType = typeEnum;
        this.column = column;
    }

    public ItemOrderBy(String columna) {
        this.orderType = OrderTypeEnum.ORDER_ASC;
        this.column = columna;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.column);
        if (this.orderType == OrderTypeEnum.ORDER_ASC) {
            sb.append(" asc");
        } else if (this.orderType == OrderTypeEnum.ORDER_DESC) {
            sb.append(" desc");
        }

        return sb.toString();
    }
}
