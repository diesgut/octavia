package pe.albatross.octavia.dynatable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.LinkedHashMap;
import java.util.Map;

public class DynatableFilter {

    private Integer page;
    private Integer perPage;
    private Integer offset;
    private Integer total;
    private Integer filtered;
    private Map sorts;
    private Map<String, Object> queries;

    public DynatableFilter() {
        sorts = new LinkedHashMap();
    }

    @JsonIgnore
    public String getSearchValue() {
        String search = "";
        if (queries != null) {
            if (queries.get("search") == null) {
                return search;
            }
            search = queries.get("search").toString();
        }
        return search;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getPerPage() {
        return perPage;
    }

    public void setPerPage(Integer perPage) {
        this.perPage = perPage;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Map getQueries() {
        return queries;
    }

    public void setQueries(Map queries) {
        this.queries = queries;
    }

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

    public Map getSorts() {
        return sorts;
    }

    public void setSorts(Map sorts) {
        this.sorts = sorts;
    }

}
