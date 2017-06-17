package pe.albatross.octavia.helpers;

import java.util.ArrayList;
import java.util.List;

public class TableQuery {

    private Boolean fetch;
    private String name;
    private String attribute;
    private String alias;
    private TypeJoinEnum type;
    private Class clazz;
    private List<TableQuery> parents;
    private TableQuery child;

    public enum TypeJoinEnum {
        INNER_JOIN, LEFT_JOIN
    };

    public TableQuery(String name, String alias, TypeJoinEnum type) {
        this.name = name;
        if (name.split("\\.").length > 1) {
            this.attribute = name.split("\\.")[1];
        }

        this.alias = alias;
        this.type = type;
        parents = new ArrayList();
        fetch = false;
    }

    public TableQuery(Class clazz, String alias) {
        this.alias = alias;
        this.clazz = clazz;
        parents = new ArrayList();
        fetch = false;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        if (name.split("\\.").length > 1) {
            this.attribute = name.split("\\.")[1];
        }
    }

    public String getAlias() {
        return alias;
    }

    public Class getClazz() {
        return clazz;
    }

    public void setClazz(Class clazz) {
        this.clazz = clazz;
    }

    public List<TableQuery> getParents() {
        return parents;
    }

    public TypeJoinEnum getType() {
        return type;
    }

    public Boolean isFetch() {
        return fetch;
    }

    public void setFetch(Boolean fetch) {
        this.fetch = fetch;
    }

    public TableQuery getChild() {
        return child;
    }

    public void setChild(TableQuery child) {
        this.child = child;
    }

    public String getAttribute() {
        return attribute;
    }

}
