package pe.albatross.octavia.dynatable;

public class DynatableResponse {

    private Integer total;
    private Integer filtered;
    private Object data;

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getFiltered() {
        return filtered;
    }

    public void setFiltered(Integer filtered) {
        this.filtered = filtered;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }



}
