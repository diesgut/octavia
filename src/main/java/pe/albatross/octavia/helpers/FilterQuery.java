package pe.albatross.octavia.helpers;

import pe.albatross.octavia.enums.ComparisonOperatorEnum;
import java.util.ArrayList;
import java.util.List;
import pe.albatross.octavia.Octavia;

public class FilterQuery {

    private String alias;
    private String column;
    private ComparisonOperatorEnum comparision;
    private Object value;
    private Object valueTwo;
    private Class columnType;
    private Integer index;
    private Integer indexTwo;
    private FilterTypeEnum filterType;
    private Octavia subQuery;
    private FilterQuery parentFilter;
    private List<FilterQuery> childrenFilters;

    public enum FilterTypeEnum {
        IS_NULL, IS_NOT_NULL, OTHER_CONDITION, //       not params
        GENERIC_OPERATOR, //                            one param
        LIKE, NOT_LIKE, COMPLEX, COMPLEX_LIKE, //       one param
        BETWEEN, BETWEEN_NOT, //                  two params
        IN_LIST, IN_LIST_DATES, //                      one list param
        NOT_IN_LIST, NOT_IN_LIST_DATES, //              one list param
        NOT_IN_QUERY, IN_QUERY, EXISTS, NOT_EXISTS, //  subquery
        BLOCK_OR, BLOCK_AND,
        UPDATE, DELETE                              //  dml
    };

    public static String getValueForLike(String value) {
        StringBuilder val = new StringBuilder("%");
        val.append(value.replaceAll(" ", "%"));
        val.append("%");
        return val.toString();
    }

    public FilterQuery(FilterQuery origin) {
        this.alias = origin.alias;
        this.column = origin.column;
        this.comparision = origin.comparision;
        this.value = origin.value;
        this.valueTwo = origin.valueTwo;
        this.columnType = origin.columnType;
        this.index = origin.index;
        this.indexTwo = origin.indexTwo;
        this.filterType = origin.filterType;
        this.subQuery = origin.subQuery;
        this.parentFilter = origin.parentFilter;

        if (origin.childrenFilters != null) {
            this.childrenFilters = new ArrayList();
            for (FilterQuery children : childrenFilters) {
                FilterQuery childrenCopy = new FilterQuery(children);
                this.childrenFilters.add(childrenCopy);
            }
        }
    }

    /**
     * Filter block
     *
     * @param parent
     */
    public FilterQuery(FilterQuery parent, FilterTypeEnum typeBlock) {
        this.filterType = typeBlock;
        this.parentFilter = parent;
        this.childrenFilters = new ArrayList();
    }

    /**
     * Filter con subquery
     *
     * @param subQuery
     */
    public FilterQuery(Octavia subQuery, FilterTypeEnum typeExists) {
        this.subQuery = subQuery;
        this.filterType = typeExists;
    }

    /**
     * Filter IN subquery
     *
     * @param subQuery
     */
    public FilterQuery(String alias, String column, Octavia subQuery, ComparisonOperatorEnum comparision, FilterTypeEnum typeExists) {
        this.alias = alias;
        this.column = column;
        this.comparision = comparision;
        this.subQuery = subQuery;
        this.filterType = typeExists;
    }

    /**
     * Filter generico
     */
    public FilterQuery(String alias, String column, ComparisonOperatorEnum comparision, Object value, Class typeColumn) {
        this.alias = alias;
        this.column = column;
        this.comparision = comparision;
        this.value = value;
        this.columnType = typeColumn;
        this.filterType = FilterTypeEnum.GENERIC_OPERATOR;
    }
    
    /**
     * Filter update
     */
    public FilterQuery(String alias, String column,  Object value) {
        this.alias = alias;
        this.column = column;
        this.comparision = ComparisonOperatorEnum.EQUAL;
        this.value = value;
        this.filterType = FilterTypeEnum.UPDATE;
    }

    /**
     * Filter para listas
     */
    public FilterQuery(String alias, String column, List values, Class typeColumn, ComparisonOperatorEnum comparision, FilterTypeEnum filterType) {
        this.alias = alias;
        this.column = column;
        this.comparision = comparision;
        this.value = values;
        this.columnType = typeColumn;
        this.filterType = filterType;
    }

    /**
     * Filter para complex
     */
    public FilterQuery(String column, ComparisonOperatorEnum comparision, Object value) {
        this.column = column;
        this.comparision = comparision;
        this.value = value;
        this.filterType = FilterTypeEnum.COMPLEX;
    }

    /**
     * Filter para otras condiciones
     */
    public FilterQuery(String columnOne, ComparisonOperatorEnum comparision, String columnTwo, boolean isOtherCondition) {
        this.column = new StringBuilder(columnOne)
                .append(" ").append(comparision.getValue()).append(" ")
                .append(columnTwo).toString();
        this.filterType = FilterTypeEnum.OTHER_CONDITION;
    }

    /**
     * Filter para LIKE
     */
    public FilterQuery(String alias, String column, String value, Class typeColumn, ComparisonOperatorEnum comparison, FilterTypeEnum filterLike) {
        if (filterLike == FilterTypeEnum.LIKE) {
            this.alias = alias;
            this.column = column;
            this.columnType = typeColumn;
        }

        if (filterLike == FilterTypeEnum.COMPLEX_LIKE) {
            this.column = column;
        }

        this.comparision = comparison;
        this.value = getValueForLike(value);
        this.filterType = filterLike;
    }

    /**
     * Filter sin comparados: IS NULL, IS NOT NULL
     */
    public FilterQuery(String alias, String column, FilterTypeEnum filterType) {
        this.alias = alias;
        this.column = column;
        this.filterType = filterType;
    }

    /**
     * Filter con dos valores
     */
    public FilterQuery(String alias, String column, Object valueOne, Object valueTwo, FilterTypeEnum filterType, Class typeColumn) {
        this.alias = alias;
        this.column = column;
        this.value = valueOne;
        this.valueTwo = valueTwo;
        this.filterType = filterType;
    }

    public Class getColumnType() {
        return columnType;
    }

    public ComparisonOperatorEnum getComparision() {
        return comparision;
    }

    public String getAlias() {
        return alias;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public Object getValueTwo() {
        return valueTwo;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public Integer getIndexTwo() {
        return indexTwo;
    }

    public void setIndexTwo(Integer indexTwo) {
        this.indexTwo = indexTwo;
    }

    public FilterTypeEnum getFilterType() {
        return filterType;
    }

    public Octavia getSubQuery() {
        return subQuery;
    }

    public FilterQuery getParentFilter() {
        return parentFilter;
    }

    public List<FilterQuery> getChildrenFilters() {
        return childrenFilters;
    }

}
