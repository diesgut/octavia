package pe.albatross.octavia.dynatable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hibernate.Session;
import pe.albatross.octavia.Octavia;
import pe.albatross.octavia.helpers.Preconditions;

public class DynatableSql {

    private Octavia totalQuery;
    private Octavia filteredQuery;
    private Octavia dataQuery;
    private Octavia subquerySearch;
    private DynatableFilter filter;
    private List<String> fields;
    private List<String> complexs;
    private List<String> subqueryFields;
    private List<String> subqueryComplexs;
    private List<List<String>> linkeds;
    private boolean isRelativeFilter;

    public DynatableSql(DynatableFilter filter) {
        this.totalQuery = Octavia.query();
        this.filteredQuery = Octavia.query();
        this.dataQuery = Octavia.query();
        this.filter = filter;
        this.fields = new ArrayList();
        this.complexs = new ArrayList();
        this.subqueryFields = new ArrayList();
        this.subqueryComplexs = new ArrayList();
        this.linkeds = new ArrayList();
        this.isRelativeFilter = false;
    }

    public DynatableSql searchFields(String... fields) {
        for (String field : fields) {
            this.fields.add(field);
        }
        return this;
    }

    public DynatableSql searchComplexField(String field) {
        this.complexs.add(field);
        return this;
    }

    public DynatableSql searchSubquery(Octavia subquery) {
        Preconditions.isTrue(subquerySearch == null, "Ya se definió el subquery principal");
        this.subquerySearch = Octavia.query();
        this.subquerySearch.copyFrom(subquery);
        return this;
    }

    public DynatableSql subqueryLinkedBy(String columnTop, String columnBottom) {
        return subqueryLinkedBy(columnTop, "=", columnBottom);
    }

    public DynatableSql subqueryLinkedBy(String columnTop, String comparision, String columnBottom) {
        Preconditions.isTrue(subquerySearch != null, "Aún no se definió el subquery principal");
        List<String> linked = Arrays.asList(columnTop, comparision, columnBottom);
        linkeds.add(linked);
        return this;
    }

    public DynatableSql searchSubqueryFields(String... fields) {
        for (String field : fields) {
            this.subqueryFields.add(field);
        }
        return this;
    }

    public DynatableSql searchSubqueryComplexField(String field) {
        this.subqueryComplexs.add(field);
        return this;
    }

    public List all(Session session) {

        if (this.totalQuery.getSelects() == null) {
            this.totalQuery.selectCount();
        }
        if (this.filteredQuery.getSelects() == null) {
            this.filteredQuery.selectCount();
        }

        String search = filter.getSearchValue();
        if (!search.equals("")) {
            String[] parts = search.split(",");
            for (String part : parts) {
                this.filteredQuery.beginBlock();
                this.dataQuery.beginBlock();

                for (String field : fields) {
                    this.filteredQuery.like(field, part);
                    this.dataQuery.like(field, part);
                }
                for (String complex : complexs) {
                    this.filteredQuery.likeComplex(complex, part);
                    this.dataQuery.likeComplex(complex, part);
                }

                if (subquerySearch != null) {
                    Octavia subqueryOne = Octavia.query();
                    Octavia subqueryTwo = Octavia.query();
                    subqueryOne.copyFrom(subquerySearch);
                    subqueryTwo.copyFrom(subquerySearch);

                    this.filteredQuery.exists(subqueryOne);
                    this.dataQuery.exists(subqueryTwo);
                    for (List<String> linked : linkeds) {
                        String columnTop = linked.get(0);
                        String comparision = linked.get(1);
                        String columnBottom = linked.get(2);

                        this.filteredQuery.linkedBy(columnTop, comparision, columnBottom);
                        this.dataQuery.linkedBy(columnTop, comparision, columnBottom);
                    }

                    if (!this.subqueryFields.isEmpty() || !this.subqueryComplexs.isEmpty()) {
                        subqueryOne.beginBlock();
                        subqueryTwo.beginBlock();
                        for (String field : this.subqueryFields) {
                            subqueryOne.like(field, part);
                            subqueryTwo.like(field, part);
                        }
                        for (String field : this.subqueryComplexs) {
                            subqueryOne.likeComplex(field, part);
                            subqueryTwo.likeComplex(field, part);
                        }
                        subqueryOne.endBlock();
                        subqueryTwo.endBlock();
                    }
                }

                this.filteredQuery.endBlock();
                this.dataQuery.endBlock();
            }
        }

        filter.setTotal(((Long) this.totalQuery.find(session)).intValue());
        filter.setFiltered(((Long) this.filteredQuery.find(session)).intValue());

        this.dataQuery.limit(filter.getOffset(), filter.getPerPage());

        return this.dataQuery.all(session);
    }

    public DynatableSql from(Class clazz) {
        totalQuery.from(clazz);
        filteredQuery.from(clazz);
        dataQuery.from(clazz);
        return this;
    }

    public DynatableSql from(Class clazz, String alias) {
        totalQuery.from(clazz, alias);
        filteredQuery.from(clazz, alias);
        dataQuery.from(clazz, alias);
        return this;
    }

    public DynatableSql join(String... tables) {
        totalQuery.join(tables);
        filteredQuery.join(tables);
        dataQuery.join(tables);
        return this;
    }

    public DynatableSql left(String... tables) {
        return this.leftJoin(tables);
    }

    public DynatableSql leftJoin(String... tables) {
        totalQuery.leftJoin(tables);
        filteredQuery.leftJoin(tables);
        dataQuery.leftJoin(tables);
        return this;
    }

    public DynatableSql beginRelativeFilters() {
        this.isRelativeFilter = true;
        return this;
    }

    public DynatableSql filter(String column, Object value) {
        filteredQuery.filter(column, value);
        dataQuery.filter(column, value);
        if (!isRelativeFilter) {
            totalQuery.filter(column, value);
        }
        return this;
    }

    public DynatableSql filter(String column, String comparision, Object value) {
        filteredQuery.filter(column, comparision, value);
        dataQuery.filter(column, comparision, value);
        if (!isRelativeFilter) {
            totalQuery.filter(column, comparision, value);
        }
        return this;
    }

    public DynatableSql in(String column, Object[] items) {
        filteredQuery.in(column, items);
        dataQuery.in(column, items);
        if (!isRelativeFilter) {
            totalQuery.in(column, items);
        }
        return this;
    }

    public DynatableSql in(String column, List values) {
        filteredQuery.in(column, values);
        dataQuery.in(column, values);
        if (!isRelativeFilter) {
            totalQuery.in(column, values);
        }
        return this;
    }

    public DynatableSql notIn(String column, List values) {
        filteredQuery.notIn(column, values);
        dataQuery.notIn(column, values);
        if (!isRelativeFilter) {
            totalQuery.notIn(column, values);
        }
        return this;
    }

    public DynatableSql filterSpecial(String columnLeft, String columnRigth) {
        filteredQuery.filterSpecial(columnLeft, columnRigth);
        dataQuery.filterSpecial(columnLeft, columnRigth);
        if (!isRelativeFilter) {
            totalQuery.filterSpecial(columnLeft, columnRigth);
        }
        return this;
    }

    public DynatableSql filterSpecial(String columnLeft, String comparision, String columnRigth) {
        filteredQuery.filterSpecial(columnLeft, comparision, columnRigth);
        dataQuery.filterSpecial(columnLeft, comparision, columnRigth);
        if (!isRelativeFilter) {
            totalQuery.filterSpecial(columnLeft, comparision, columnRigth);
        }
        return this;
    }

    public DynatableSql like(String column, String value) {
        filteredQuery.like(column, value);
        dataQuery.like(column, value);
        if (!isRelativeFilter) {
            totalQuery.like(column, value);
        }
        return this;
    }

    public DynatableSql notLike(String column, String value) {
        filteredQuery.notLike(column, value);
        dataQuery.notLike(column, value);
        if (!isRelativeFilter) {
            totalQuery.notLike(column, value);
        }
        return this;
    }

    public DynatableSql isNull(String column) {
        filteredQuery.isNull(column);
        dataQuery.isNull(column);
        if (!isRelativeFilter) {
            totalQuery.isNull(column);
        }
        return this;
    }

    public DynatableSql isNotNull(String column) {
        filteredQuery.isNotNull(column);
        dataQuery.isNotNull(column);
        if (!isRelativeFilter) {
            totalQuery.isNotNull(column);
        }
        return this;
    }

    public DynatableSql complexFilter(String complex, Object value) {
        filteredQuery.complexFilter(complex, value);
        dataQuery.complexFilter(complex, value);
        if (!isRelativeFilter) {
            totalQuery.complexFilter(complex, value);
        }
        return this;
    }

    public DynatableSql complexFilter(String complex, String comparision, Object value) {
        filteredQuery.complexFilter(complex, comparision, value);
        dataQuery.complexFilter(complex, comparision, value);
        if (!isRelativeFilter) {
            totalQuery.complexFilter(complex, comparision, value);
        }
        return this;
    }

    public DynatableSql likeComplex(String complex, String value) {
        filteredQuery.likeComplex(complex, value);
        dataQuery.likeComplex(complex, value);
        if (!isRelativeFilter) {
            totalQuery.likeComplex(complex, value);
        }
        return this;
    }

    public DynatableSql betweenIn(String column, Object valueMin, Object valueMax) {
        filteredQuery.between(column, valueMin, valueMax);
        dataQuery.between(column, valueMin, valueMax);
        if (!isRelativeFilter) {
            totalQuery.between(column, valueMin, valueMax);
        }
        return this;
    }

    public DynatableSql betweenNotIn(String column, Object valueMin, Object valueMax) {
        filteredQuery.notBetween(column, valueMin, valueMax);
        dataQuery.notBetween(column, valueMin, valueMax);
        if (!isRelativeFilter) {
            totalQuery.notBetween(column, valueMin, valueMax);
        }
        return this;
    }

    public DynatableSql exists(Octavia subQuery) {
        Octavia sqlCopyA = Octavia.query();
        sqlCopyA.copyFrom(subQuery);
        Octavia sqlCopyB = Octavia.query();
        sqlCopyB.copyFrom(subQuery);

        filteredQuery.exists(sqlCopyA);
        dataQuery.exists(sqlCopyB);
        if (!isRelativeFilter) {
            Octavia sqlCopyC = Octavia.query();
            sqlCopyC.copyFrom(subQuery);
            totalQuery.exists(sqlCopyC);
        }
        return this;
    }

    public DynatableSql notExists(Octavia subQuery) {
        Octavia sqlCopyA = Octavia.query();
        sqlCopyA.copyFrom(subQuery);
        Octavia sqlCopyB = Octavia.query();
        sqlCopyB.copyFrom(subQuery);

        filteredQuery.notExists(sqlCopyA);
        dataQuery.notExists(sqlCopyB);
        if (!isRelativeFilter) {
            Octavia sqlCopyC = Octavia.query();
            sqlCopyC.copyFrom(subQuery);
            totalQuery.notExists(sqlCopyC);
        }
        return this;
    }

    public DynatableSql linkedBy(String columnTop, String columnBottom) {
        filteredQuery.linkedBy(columnTop, columnBottom);
        dataQuery.linkedBy(columnTop, columnBottom);
        if (!isRelativeFilter) {
            totalQuery.linkedBy(columnTop, columnBottom);
        }
        return this;
    }

    public DynatableSql linkedBy(String columnTop, String comparision, String columnBottom) {
        filteredQuery.linkedBy(columnTop, comparision, columnBottom);
        dataQuery.linkedBy(columnTop, comparision, columnBottom);
        if (!isRelativeFilter) {
            totalQuery.linkedBy(columnTop, comparision, columnBottom);
        }
        return this;
    }

    public DynatableSql in(String column, Octavia subQuery) {
        Octavia sqlCopyA = Octavia.query();
        sqlCopyA.copyFrom(subQuery);
        Octavia sqlCopyB = Octavia.query();
        sqlCopyB.copyFrom(subQuery);

        filteredQuery.in(column, sqlCopyA);
        dataQuery.in(column, sqlCopyB);
        if (!isRelativeFilter) {
            Octavia sqlCopyC = Octavia.query();
            sqlCopyC.copyFrom(subQuery);
            totalQuery.in(column, sqlCopyC);
        }

        return this;
    }

    public DynatableSql notIn(String column, Octavia subQuery) {
        Octavia sqlCopyA = Octavia.query();
        sqlCopyA.copyFrom(subQuery);
        Octavia sqlCopyB = Octavia.query();
        sqlCopyB.copyFrom(subQuery);

        filteredQuery.notIn(column, sqlCopyA);
        dataQuery.notIn(column, sqlCopyB);
        if (!isRelativeFilter) {
            Octavia sqlCopyC = Octavia.query();
            sqlCopyC.copyFrom(subQuery);
            totalQuery.notIn(column, sqlCopyC);
        }

        return this;
    }

    public DynatableSql beginBlock() {
        filteredQuery.beginBlock();
        dataQuery.beginBlock();
        if (!isRelativeFilter) {
            totalQuery.beginBlock();
        }
        return this;
    }

    public DynatableSql endBlock() {
        filteredQuery.endBlock();
        dataQuery.endBlock();
        if (!isRelativeFilter) {
            totalQuery.endBlock();
        }
        return this;
    }

    public DynatableSql selectDistinct(String... fields) {
        dataQuery.selectDistinct(fields);
        filteredQuery.selectCountDistinct(fields);
        totalQuery.selectCountDistinct(fields);
        return this;
    }

    public DynatableSql select(String... fields) {
        dataQuery.select(fields);
        filteredQuery.selectCount();
        totalQuery.selectCount();
        return this;
    }

    public DynatableSql into(Class clazz) {
        dataQuery.into(clazz);
        return this;
    }

    public DynatableSql orderBy(String... items) {
        dataQuery.orderBy(items);
        return this;
    }

    public DynatableSql groupBy(String... items) {
        dataQuery.groupBy(items);
        return this;
    }

    public DynatableSql limit(Integer firstResult, Integer maxResults) {
        dataQuery.limit(firstResult, maxResults);
        return this;
    }

    public DynatableSql limit(Integer maxResults) {
        dataQuery.limit(maxResults);
        return this;
    }

    public DynatableSql __() {
        return this;
    }

    public DynatableSql $$$() {
        System.out.println("*****************************************************************************");
        return this;
    }
}
