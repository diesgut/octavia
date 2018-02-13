package pe.albatross.octavia;

import pe.albatross.octavia.helpers.TableQuery;
import pe.albatross.octavia.helpers.ItemOrderBy;
import pe.albatross.octavia.helpers.FilterQuery;
import pe.albatross.octavia.enums.ComparisonOperatorEnum;
import pe.albatross.octavia.helpers.Preconditions;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hibernate.Session;
import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pe.albatross.octavia.exceptions.OctaviaException;
import static pe.albatross.octavia.helpers.FilterQuery.FilterTypeEnum.*;
import pe.albatross.octavia.helpers.ItemOrderBy.OrderTypeEnum;
import pe.albatross.octavia.helpers.ObjectHelper;
import pe.albatross.octavia.helpers.TableQuery.TypeJoinEnum;

public class Octavia {

    private String selects;
    private String groupBys;
    private String existsStatus;
    private Integer aliasCount;
    private Integer firstResult;
    private Integer maxResults;
    private Integer parametersCount;
    private Integer selectsCount;
    private TableQuery mainTable;
    private Class clazzInto;
    private Query query;
    private String prefix;
    private Octavia activeSubquery;
    private FilterQuery blockFilter;
    private List<FilterQuery> childrenFilters;

    private Boolean distinct;
    private Boolean count;
    private Boolean isUpdate;
    private Boolean isClauseWhereOpen;
    private Boolean isSubqueryIncluded;
    private Boolean isClauseExistsOpen;
    private Boolean isClauseInQueryOpen;

    private List<FilterQuery> filters;
    private List<FilterQuery> sets;
    private List<ItemOrderBy> itemsOrderBy;
    private Map<String, TableQuery> mapQueryTables;

    private final String separator = "::::::::";
    private final String enter = " \n";
    private final String enterInto = " \n\t";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static List<Class> TYPICAL_CLASSES = Arrays.asList(
            String.class, Integer.class, Long.class, BigDecimal.class, Float.class, Double.class, Timestamp.class, Date.class
    );

    public Octavia() {
        filters = new ArrayList();
        sets = new ArrayList();
        itemsOrderBy = new ArrayList();
        mapQueryTables = new LinkedHashMap();
        prefix = "";
        parametersCount = 1;
        selectsCount = 0;
        blockFilter = null;
        childrenFilters = null;
        childrenFilters = null;

        isClauseWhereOpen = false;
        isSubqueryIncluded = false;
        isClauseExistsOpen = false;
        isClauseInQueryOpen = false;
        isUpdate = false;
    }

    public static Octavia query() {
        return new Octavia();
    }

    public static Octavia query(Class clazz) {
        Octavia octavia = new Octavia();
        octavia.from(clazz);
        return octavia;
    }

    public static Octavia query(Class clazz, String alias) {
        Octavia octavia = new Octavia();
        octavia.from(clazz, alias);
        return octavia;
    }

    public static Octavia update() {
        Octavia octavia = new Octavia();
        octavia.isUpdate = true;
        return octavia;
    }

    public static Octavia update(Class clazz) {
        Octavia octavia = new Octavia();
        octavia.isUpdate = true;
        octavia.from(clazz);
        return octavia;
    }

    public static Octavia update(Class clazz, String alias) {
        Octavia octavia = new Octavia();
        octavia.isUpdate = true;
        octavia.from(clazz, alias);
        return octavia;
    }

    public Octavia set(Object val, String... columns) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isTrue(columns.length > 0, "No indicó ningún elemento en el SET");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            Preconditions.isNotBlank(column, "Debe indicar correctament el elemento " + (i + 1) + " del SET");
            Preconditions.isFalse(isFormula(column), "El elemento " + (i + 1) + " del SET es una formula");
        }

        for (String field : columns) {
            field = field.trim();
            getAttribute(field);
            String[] parts = field.split("\\.");
            String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
            String attribute = parts.length == 2 ? parts[1] : parts[0];

            FilterQuery fq = new FilterQuery(alias, attribute, ObjectHelper.getParentTree(val, field));
            fq.setIndex(parametersCount++);

            sets.add(fq);

        }

        this.filter("id", val);
        return this;
    }

    public void increaseParams(Integer beginCount, List<FilterQuery> filters) {
        for (FilterQuery filter : filters) {
            switch (filter.getFilterType()) {
                case GENERIC_OPERATOR:
                case COMPLEX:
                case COMPLEX_LIKE:
                case LIKE:
                case NOT_LIKE:
                    filter.setIndex(filter.getIndex() + beginCount);
                    break;
                case IN_LIST:
                case IN_LIST_DATES:
                case NOT_IN_LIST:
                case NOT_IN_LIST_DATES:
                    filter.setIndex(filter.getIndex() + beginCount);
                    break;
                case IN_QUERY:
                case NOT_IN_QUERY:
                case EXISTS:
                case NOT_EXISTS:
                    filter.setIndex(filter.getIndex() + beginCount);
                    break;
                case BETWEEN_IN:
                case BETWEEN_NOT_IN:
                    filter.setIndex(filter.getIndex() + beginCount);
                    filter.setIndexTwo(filter.getIndexTwo() + beginCount);
                    break;
                case BLOCK_AND:
                case BLOCK_OR:
                    filter.setIndex(filter.getIndex() + beginCount);
                    increaseParams(beginCount, filter.getChildrenFilters());
                    break;
                default:
                    break;
            }
        }
        this.parametersCount = this.parametersCount + beginCount;
    }

    public Boolean isSubqueryIncluded() {
        return isSubqueryIncluded;
    }

    public void setIsSubqueryIncluded(Boolean isSubqueryIncluded) {
        this.isSubqueryIncluded = isSubqueryIncluded;
    }

    public Integer getParametersCount() {
        return parametersCount;
    }

    public List<FilterQuery> getFilters() {
        return filters;
    }

    public String getSelects() {
        return selects;
    }

    public void setIsClauseExistsOpen(Boolean isClauseExistsOpen) {
        this.isClauseExistsOpen = isClauseExistsOpen;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public Integer getFirstResult() {
        return firstResult;
    }

    public Integer getMaxResults() {
        return maxResults;
    }

    public Octavia from(Class clazz) {
        String alias = getNewAlias();
        Preconditions.isNull(mainTable, "Ya existe asignada la tabla principal");
        mainTable = new TableQuery(clazz, alias);
        mapQueryTables.put(alias, mainTable);
        return this;
    }

    public Octavia from(Class clazz, String alias) {
        Preconditions.isNull(mainTable, "Ya existe asignada la tabla principal");
        mainTable = new TableQuery(clazz, alias);
        mapQueryTables.put(alias, mainTable);
        return this;
    }

    private String getNewAlias() {
        if (aliasCount == null) {
            aliasCount = 0;
        }
        aliasCount++;
        return "_aliasTb_" + String.format("%06d", aliasCount);
    }

    private void addParent(TableQuery parent) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Class clazz;
        Class classMain = mainTable.getClazz();
        TableQuery previous = mapQueryTables.get(parent.getAlias());
        Preconditions.isTrue(previous == null, "El alias de [[" + parent.getName() + "]] ya se encuentra utilizado");

        String[] parts = parent.getName().split("\\.");
        Preconditions.isFalse(parts.length > 2, "El join [[" + parent.getName() + "]] tiene muchos alias padres");

        TableQuery child;
        if (parts.length == 2) {
            child = mapQueryTables.get(parts[0]);
            Preconditions.isNotNull(child, "No existe una tabla hija con el alias [[" + parts[0] + "]]");
            String err = "No existe el padre [[" + parts[1] + "]] para el alias [[" + parts[0] + "]]";
            clazz = getField(child.getClazz(), parts[1], err).getType();
        } else {
            child = mainTable;
            String err = "No existe el padre [[" + parts[0] + "]] para la tabla [[" + classMain.getSimpleName() + "]]";
            clazz = getField(classMain, parts[0], err).getType();
            parent.setName(mainTable.getAlias() + "." + parent.getName());
        }

        parent.setClazz(clazz);
        parent.setChild(child);
        mapQueryTables.put(parent.getAlias(), parent);
        child.getParents().add(parent);
    }

    private Octavia join(TypeJoinEnum typeJoin, String... tables) {
        for (String table : tables) {
            table = table.replaceAll("^ +| +$|( )+", "$1");
            String[] parts = table.split(" ");
            Preconditions.isFalse(parts.length > 2, "El join [[" + table + "]] tiene muchos parámetros");
            if (parts.length == 2) {
                addParent(new TableQuery(parts[0], parts[1], typeJoin));
            } else {
                addParent(new TableQuery(parts[0], getNewAlias(), typeJoin));
            }
        }
        return this;
    }

    public Octavia join(String... tables) {
        return join(TypeJoinEnum.INNER_JOIN, tables);
    }

    public Octavia left(String... tables) {
        return this.leftJoin(tables);
    }

    public Octavia leftJoin(String... tables) {
        return join(TypeJoinEnum.LEFT_JOIN, tables);
    }

    public Octavia __() {
        return this;
    }

    public Octavia filter(String column, Object value) {
        if (value instanceof Enum) {
            return filter(column, "=", ((Enum) value).name());
        } else {
            return filter(column, "=", value);
        }
    }

    public Octavia filter(String column, String comparision, Object value) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotNull(value, "El valor de comparación es null");
        Preconditions.isNotBlank(column, "La columna del filter es incorrecta");
        Preconditions.isNotBlank(comparision, "El signo de comparación del filter es incorrecto");
        column = column.trim();

        value = (value instanceof Enum) ? ((Enum) value).name() : value;

        String[] parts = column.split("\\.");
        Field fieldAttr = getAttribute(column);
        String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
        String attribute = parts.length == 2 ? parts[1] : parts[0];

        FilterQuery fq = new FilterQuery(alias, attribute, findComparison(comparision), value, fieldAttr.getType());
        fq.setIndex(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia in(String column, Object[] items) {
        List list = new ArrayList();
        for (Object item : items) {
            if (item instanceof Enum) {
                list.add(((Enum) item).name());
            } else {
                list.add(item);
            }
        }
        return setFilterIn(column, list, ComparisonOperatorEnum.IN, IN_LIST);
    }

    public Octavia in(String column, List values) {
        List valuex = new ArrayList();
        for (Object item : values) {
            if (item instanceof Enum) {
                valuex.add(((Enum) item).name());
            } else {
                valuex.add(item);
            }
        }
        return setFilterIn(column, valuex, ComparisonOperatorEnum.IN, IN_LIST);
    }

    public Octavia notIn(String column, List values) {
        return setFilterIn(column, values, ComparisonOperatorEnum.NOT_IN, NOT_IN_LIST);
    }

    private Octavia setFilterIn(String column, List values, ComparisonOperatorEnum comparison, FilterQuery.FilterTypeEnum typeFilter) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotBlank(column, "La columna del filter es incorrecta");
        column = column.trim();

        String[] parts = column.split("\\.");
        Field fieldAttr = getAttribute(column);
        String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
        String attribute = parts.length == 2 ? parts[1] : parts[0];

        if (values.isEmpty()) {
            String value2 = (typeFilter == IN_LIST) ? "2" : "1";
            FilterQuery fq = new FilterQuery("1", ComparisonOperatorEnum.EQUAL, value2, true);
            fq.setIndex(parametersCount++);
            if (blockFilter != null) {
                this.childrenFilters.add(fq);
            } else {
                filters.add(fq);
            }
            return this;
        }

        Class clazz = values.get(0).getClass();
        if (clazz == String.class && fieldAttr.getType() == Date.class) {
            FilterQuery.FilterTypeEnum typeDate
                    = (typeFilter == IN_LIST)
                            ? IN_LIST_DATES
                            : NOT_IN_LIST_DATES;

            FilterQuery fq = new FilterQuery(alias, attribute, values, clazz, comparison, typeDate);
            fq.setIndex(parametersCount++);

            if (blockFilter != null) {
                this.childrenFilters.add(fq);
            } else {
                filters.add(fq);
            }

            return this;
        }

        if (TYPICAL_CLASSES.contains(clazz)) {

            FilterQuery fq = new FilterQuery(alias, attribute, values, clazz, comparison, typeFilter);
            fq.setIndex(parametersCount++);

            if (blockFilter != null) {
                this.childrenFilters.add(fq);
            } else {
                filters.add(fq);
            }

            return this;
        }

        List<Long> ids = new ArrayList();
        for (Object item : values) {
            Long id = getIdObject(item);
            ids.add(id);
        }

        FilterQuery fq = new FilterQuery(alias, attribute, ids, clazz, comparison, typeFilter);
        fq.setIndex(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia filterSpecial(String columnLeft, String columnRigth) {
        return filterSpecial(columnLeft, "=", columnRigth);
    }

    public Octavia filterSpecial(String columnLeft, String comparision, String columnRigth) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotBlank(columnLeft, "La columna 1 del filter es incorrecta");
        Preconditions.isNotBlank(columnRigth, "La columna 2 del filter es incorrecta");
        Preconditions.isTrue(isCorrectFormula(columnLeft), "La columna compleja 1 es incorrecta");
        Preconditions.isTrue(isCorrectFormula(columnRigth), "La columna compleja 2 es incorrecta");
        Preconditions.isNotBlank(comparision, "El signo de comparación del filter es incorrecto");
        columnLeft = columnLeft.trim();
        columnRigth = columnRigth.trim();

        if (!isFormula(columnLeft)) {
            getAttribute(columnLeft);
        }
        if (!isFormula(columnRigth)) {
            getAttribute(columnRigth);
        }

        FilterQuery fq = new FilterQuery(columnLeft.trim(), findComparison(comparision), columnRigth.trim(), true);
        fq.setIndex(parametersCount++);
        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia like(String column, String value) {
        return setlike(column, value, ComparisonOperatorEnum.LIKE);
    }

    public Octavia notLike(String column, String value) {
        return setlike(column, value, ComparisonOperatorEnum.NOT_LIKE);
    }

    private Octavia setlike(String column, String value, ComparisonOperatorEnum comparison) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotNull(value, "El valor de comparación es null", false);
        Preconditions.isNotBlank(column, "La columna del filter es incorrecta");
        column = column.trim();

        getAttribute(column);
        String[] parts = column.split("\\.");
        Field fieldAttr = getAttribute(column);
        String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
        String attribute = parts.length == 2 ? parts[1] : parts[0];

        FilterQuery fq = new FilterQuery(alias, attribute, value, fieldAttr.getType(), comparison, FilterQuery.FilterTypeEnum.LIKE);
        fq.setIndex(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia isNull(String column) {
        return setIsNull(column, IS_NULL);
    }

    public Octavia isNotNull(String column) {
        return setIsNull(column, IS_NOT_NULL);
    }

    private Octavia setIsNull(String column, FilterQuery.FilterTypeEnum type) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotBlank(column, "La columna del IS_NOT_NULL es incorrecta");
        column = column.trim();

        String[] parts = column.split("\\.");
        getAttribute(column);
        String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
        String attribute = parts.length == 2 ? parts[1] : parts[0];

        FilterQuery fq = new FilterQuery(alias, attribute, type);
        fq.setIndex(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia complexFilter(String complex, Object value) {
        return complexFilter(complex, "=", value);
    }

    public Octavia complexFilter(String complex, String comparision, Object value) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotNull(value, "El valor de comparación es null");
        Preconditions.isNotBlank(complex, "La columna del filter es incorrecta");
        Preconditions.isTrue(isCorrectFormula(complex), "La columna compleja es incorrecta");
        Preconditions.isNotBlank(comparision, "El signo de comparación del filter es incorrecto");
        complex = complex.trim();

        if (!isFormula(complex)) {
            getAttribute(complex);
        }

        FilterQuery fq = new FilterQuery(complex, findComparison(comparision), value);
        fq.setIndex(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia likeComplex(String complex, String value) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotNull(value, "El valor de comparación es null", false);
        Preconditions.isNotBlank(complex, "La columna del filter es incorrecta");
        Preconditions.isTrue(isCorrectFormula(complex), "La columna compleja es incorrecta");
        complex = complex.trim();

        if (!isFormula(complex)) {
            getAttribute(complex);
        }

        FilterQuery fq = new FilterQuery(null, complex, value, null, findComparison("like"), FilterQuery.FilterTypeEnum.COMPLEX_LIKE);
        fq.setIndex(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia betweenIn(String column, Object valueMin, Object valueMax) {
        return setBetween(column, valueMin, valueMax, BETWEEN_IN);
    }

    public Octavia betweenNotIn(String column, Object valueMin, Object valueMax) {
        return setBetween(column, valueMin, valueMax, BETWEEN_NOT_IN);
    }

    private Octavia setBetween(String column, Object valueMin, Object valueMax, FilterQuery.FilterTypeEnum typeBetween) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotNull(valueMin, "El valor mínimo de la clausula between es null");
        Preconditions.isNotNull(valueMax, "El valor máximo de la clausula between es null");
        Preconditions.isNotBlank(column, "La columna del beteewn es incorrecta");
        column = column.trim();

        String[] parts = column.split("\\.");
        Field fieldAttr = getAttribute(column);
        String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
        String attribute = parts.length == 2 ? parts[1] : parts[0];

        FilterQuery fq = new FilterQuery(alias, attribute, valueMin, valueMax, typeBetween, fieldAttr.getType());
        fq.setIndex(parametersCount++);
        fq.setIndexTwo(parametersCount++);

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        return this;
    }

    public Octavia exists(Octavia subQuery) {
        return setExists(subQuery, EXISTS);
    }

    public Octavia notExists(Octavia subQuery) {
        return setExists(subQuery, NOT_EXISTS);
    }

    private Octavia setExists(Octavia subQuery, FilterQuery.FilterTypeEnum typeExists) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isTrue(existsStatus == null || existsStatus.equals("CONNECTED"), "Existe una clausula EXISTS sin conexion al query superior");
        Preconditions.isTrue(!subQuery.isSubqueryIncluded(), "El subquery de la clausula EXISTS ya fue incluida");

        subQuery.setPrefix(this.prefix + (blockFilter != null ? "\t" : "") + "\t");
        FilterQuery fq = new FilterQuery(subQuery, typeExists);
        fq.setIndex(parametersCount++);

        subQuery.setIsClauseExistsOpen(true);
        existsStatus = "OPEN";
        isClauseInQueryOpen = false;

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        activeSubquery = subQuery;
        subQuery.increaseParams(this.parametersCount - 1, subQuery.filters);
        subQuery.setIsSubqueryIncluded(true);
        this.parametersCount = subQuery.getParametersCount();

        return this;
    }

    public Octavia linkedBy(String columnTop, String columnBottom) {
        return linkedBy(columnTop, "=", columnBottom);
    }

    public Octavia linkedBy(String columnTop, String comparision, String columnBottom) {
        Preconditions.isNotNull(existsStatus, "No existe una clausula EXISTS abierta");
        Preconditions.isNotBlank(columnTop, "La columna top del EXISTS es incorrecta");
        Preconditions.isNotBlank(columnBottom, "La columna bottom del EXISTS es incorrecta");
        columnTop = columnTop.trim();
        columnBottom = columnBottom.trim();

        getAttribute(columnTop);
        activeSubquery.getAttribute(columnBottom);

        FilterQuery fq = new FilterQuery(columnTop, findComparison(comparision), columnBottom, true);
        fq.setIndex(activeSubquery.parametersCount++);

        activeSubquery.getFilters().add(fq);
        existsStatus = "CONNECTED";
        return this;
    }

    public Octavia in(String column, Octavia subQuery) {
        return setInQuery(column, subQuery, ComparisonOperatorEnum.IN, IN_QUERY);
    }

    public Octavia notIn(String column, Octavia subQuery) {
        return setInQuery(column, subQuery, ComparisonOperatorEnum.NOT_IN, NOT_IN_QUERY);
    }

    private Octavia setInQuery(String column, Octavia subQuery, ComparisonOperatorEnum comparison, FilterQuery.FilterTypeEnum typeInQuery) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isNotBlank(column, "La columna del filter es incorrecta");
        Preconditions.isTrue(!subQuery.isSubqueryIncluded(), "El subquery de la clausula IN QUERY ya fue incluida");
        Preconditions.isNotNull(subQuery.getSelects(), "Debe incluir la clausula SELECT en el subquery IN QUERY");
        column = column.trim();

        String[] parts = column.split("\\.");
        getAttribute(column);
        String alias = parts.length == 2 ? parts[0] : mainTable.getAlias();
        String attribute = parts.length == 2 ? parts[1] : parts[0];

        subQuery.setPrefix(this.prefix + (blockFilter != null ? "\t" : "") + "\t");
        FilterQuery fq = new FilterQuery(alias, attribute, subQuery, comparison, typeInQuery);
        fq.setIndex(parametersCount++);
        isClauseInQueryOpen = true;

        if (blockFilter != null) {
            this.childrenFilters.add(fq);
        } else {
            filters.add(fq);
        }

        activeSubquery = subQuery;
        subQuery.increaseParams(this.parametersCount - 1, subQuery.filters);
        subQuery.setIsSubqueryIncluded(true);
        this.parametersCount = subQuery.getParametersCount();

        return this;
    }

    public Octavia beginBlock() {
        FilterQuery.FilterTypeEnum typeBlock;
        if (this.blockFilter == null) {
            typeBlock = BLOCK_OR;
        } else if (this.blockFilter.getFilterType() == BLOCK_OR) {
            typeBlock = BLOCK_AND;
        } else {
            typeBlock = BLOCK_OR;
        }

        FilterQuery fq = new FilterQuery(blockFilter, typeBlock);
        fq.setIndex(parametersCount++);

        if (blockFilter == null) {
            this.filters.add(fq);
        } else {
            this.childrenFilters.add(fq);
        }
        this.blockFilter = fq;
        this.childrenFilters = fq.getChildrenFilters();

        return this;
    }

    public Octavia endBlock() {
        Preconditions.isTrue(blockFilter != null, "No existe ningún bloque sql para cerrar");
        Preconditions.isFalse(childrenFilters.isEmpty(), "No se han agregado elementos al bloque");

        FilterQuery fq = this.blockFilter.getParentFilter();
        this.blockFilter = fq;
        this.childrenFilters = (fq == null) ? null : fq.getChildrenFilters();

        return this;
    }

    private Octavia setSelect(boolean distinct, boolean count, String... columns) {
        Preconditions.isNull(selects, "Ya indicó la clausula SELECT");
        Preconditions.isTrue(columns.length > 0, "No indicó ningún elemento en el SELECT");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            Preconditions.isNotBlank(column, "Debe indicar correctament el elemento " + (i + 1) + " del Select");
            Preconditions.isTrue(isCorrectFormula(column), "El elemento " + (i + 1) + " del Select es una formula incorrecta");
        }

        this.selects = "";
        for (String field : columns) {
            field = field.trim();
            this.selects += selects.equals("") ? "" : separator;
            this.selects += field;
        }
        this.distinct = distinct;
        this.count = count;
        this.selectsCount = columns.length;
        return this;
    }

    public Octavia selectDistinct(String... fields) {
        return setSelect(true, false, fields);
    }

    public Octavia selectCountDistinct(String... fields) {
        return setSelect(true, true, fields);
    }

    public Octavia select(String... fields) {
        return setSelect(false, false, fields);
    }

    public Octavia selectCount() {
        Preconditions.isNull(selects, "Ya indicó la clausula SELECT");
        distinct = false;
        count = true;
        selects = "*";
        return this;
    }

    public Octavia into(Class clazz) {
        Preconditions.isNotNull(selects, "Debe indicar la clausula SELECT");
        Preconditions.isNull(clazzInto, "Ya indicó la clausula INTO para el SELECT");
        clazzInto = clazz;
        return this;
    }

    public Octavia orderBy(String... items) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isTrue(items.length > 0, "No indicó ningún elemento en el ORDER BY");

        for (int i = 0; i < items.length; i++) {
            String item = items[i];
            Preconditions.isNotBlank(item, "Debe indicar correctament el elemento " + (i + 1) + " del ORDER BY");
            item = item.trim();
            String column = item;
            OrderTypeEnum type = OrderTypeEnum.ORDER_ASC;
            if (item.toUpperCase().endsWith(" DESC")) {
                column = item.substring(0, item.length() - 5).trim();
                type = OrderTypeEnum.ORDER_DESC;
            } else if (item.toUpperCase().endsWith(" ASC")) {
                column = item.substring(0, item.length() - 4).trim();
            }

            Preconditions.isTrue(isCorrectFormula(column), "El elemento " + (i + 1) + " del ORDER BY es una formula incorrecta");
            if (isNumeric(column) && selectsCount > 0) {
                int position = Integer.valueOf(column);
                Preconditions.isFalse(position < 0, "El elemento " + (i + 1) + " del ORDER BY no puede ser NEGATIVO");
                Preconditions.isFalse(position == 0, "El elemento " + (i + 1) + " del ORDER BY no puede ser CERO");
                Preconditions.isTrue(position <= selectsCount, "El elemento " + (i + 1) + " del ORDER BY apunta ordenar una columna que no existe");
            }
            if (!isNumeric(column) && !isFormula(column)) {
                getAttribute(column);
            }

            itemsOrderBy.add(new ItemOrderBy(column, type));

        }

        return this;
    }

    public Octavia groupBy(String... items) {
        Preconditions.isNotNull(mainTable, "Aun no existe la tabla principal");
        Preconditions.isTrue(items.length > 0, "No indicó ningún elemento en el GROUP BY");

        groupBys = groupBys == null ? "" : groupBys;

        for (int i = 0; i < items.length; i++) {
            String item = items[i];
            Preconditions.isNotBlank(item, "Debe indicar correctament el elemento " + (i + 1) + " del GROUP BY");
            item = item.trim();
            Preconditions.isTrue(isCorrectFormula(item), "El elemento " + (i + 1) + " del ORDER BY es una formula incorrecta");
            if (!isFormula(item)) {
                getAttribute(item);
            }

            groupBys += groupBys.equals("") ? "" : ", ";
            groupBys += item;
        }

        return this;
    }

    public Octavia limit(Integer firstResult, Integer maxResults) {
        this.firstResult = firstResult;
        this.maxResults = maxResults;
        return this;
    }

    public Octavia limit(Integer maxResults) {
        return limit(0, maxResults);
    }

    public Integer execute(Session session) {
        query = session.createQuery(toString());
        setValuesSet(sets);
        setValues(filters);
        return query.executeUpdate();
    }

    public Object find(Session session) {
        query = session.createQuery(toString());
        setValues(filters);
        if (firstResult != null) {
            query.setFirstResult(firstResult);
        }
        if (maxResults != null) {
            query.setMaxResults(maxResults);
        }
        return query.uniqueResult();
    }

    public List all(Session session) {
        query = session.createQuery(toString());
        setValues(filters);
        if (firstResult != null) {
            query.setFirstResult(firstResult);
        }
        if (maxResults != null) {
            query.setMaxResults(maxResults);
        }
        return query.list();
    }

    private void setValuesSet(List<FilterQuery> sets) {
        for (FilterQuery filter : sets) {
            setValue(filter);
        }
    }

    private void setValues(List<FilterQuery> filters) {
        for (FilterQuery filter : filters) {
            logger.debug("filter: " + filter.getFilterType().name());
            switch (filter.getFilterType()) {
                case GENERIC_OPERATOR:
                case COMPLEX:
                    if (filter.getComparision() == ComparisonOperatorEnum.IN
                            || filter.getComparision() == ComparisonOperatorEnum.NOT_IN) {
                        setListValuesComplex(filter);
                    } else {
                        setValue(filter);
                    }
                    break;
                case COMPLEX_LIKE:
                case LIKE:
                case NOT_LIKE:
                    setValue(filter);
                    break;
                case IN_LIST:
                case IN_LIST_DATES:
                case NOT_IN_LIST:
                case NOT_IN_LIST_DATES:
                    setListValues(filter);
                    break;
                case BETWEEN_IN:
                case BETWEEN_NOT_IN:
                    setTwoValues(filter);
                    break;
                case IN_QUERY:
                case NOT_IN_QUERY:
                case EXISTS:
                case NOT_EXISTS:
                    filter.getSubQuery().query = query;
                    filter.getSubQuery().setValues(filter.getSubQuery().filters);
                    break;
                case BLOCK_AND:
                case BLOCK_OR:
                    setValues(filter.getChildrenFilters());
                    break;
                default:
                    break;
            }
        }
    }

    private void setListValuesComplex(FilterQuery filter) {
        List values = getListValuesComplex(filter.getValue());
        String param = "PARAM_" + String.format("%06d", filter.getIndex());
        query.setParameterList(param, values);
    }

    private List getListValuesComplex(Object value) {

        if (value instanceof List) {
            List values = (List) value;
            if (values.isEmpty()) {
                return null;
            }
            Class clazz = values.get(0).getClass();
            if (TYPICAL_CLASSES.contains(clazz)) {
                return values;
            }
            List<Long> ids = new ArrayList();
            for (Object item : values) {
                Long id = getIdObject(item);
                ids.add(id);
            }
            return ids;

        }

        if (!(value instanceof Object[])) {
            return null;
        }

        List values = new ArrayList();
        if (value instanceof Object[]) {
            Object[] items = (Object[]) value;
            for (Object item : items) {
                if (item instanceof Enum) {
                    values.add(((Enum) item).name());
                } else {
                    values.add(item);
                }
            }
            if (values.isEmpty()) {
                return null;
            }
        }
        return values;
    }

    private void setListValues(FilterQuery filter) {
        List values = (List) filter.getValue();
        String param = "PARAM_" + String.format("%06d", filter.getIndex());
        query.setParameterList(param, values);
    }

    private void setValue(FilterQuery filter) {
        Object value = filter.getValue();
        String param = "PARAM_" + String.format("%06d", filter.getIndex());

        if (value instanceof String) {
            query.setString(param, (String) value);
        } else if (value instanceof Integer) {
            query.setInteger(param, (Integer) value);
        } else if (value instanceof Long) {
            query.setLong(param, (Long) value);
        } else if (value instanceof BigDecimal) {
            query.setBigDecimal(param, (BigDecimal) value);
        } else if (value instanceof Float) {
            query.setFloat(param, (Float) value);
        } else if (value instanceof Double) {
            query.setDouble(param, (Double) value);
        } else if (value instanceof Timestamp) {
            query.setTimestamp(param, (Timestamp) value);
        } else if (value instanceof Date) {
            query.setDate(param, (Date) value);
        } else {
            query.setLong(param, getIdObject(value));
        }
    }

    private void setTwoValues(FilterQuery filter) {
        setValue(filter);

        Object valueTwo = filter.getValueTwo();
        String paramTwo = "PARAM_" + String.format("%06d", filter.getIndexTwo());

        if (valueTwo instanceof String) {
            query.setString(paramTwo, (String) valueTwo);
        } else if (valueTwo instanceof Integer) {
            query.setInteger(paramTwo, (Integer) valueTwo);
        } else if (valueTwo instanceof Long) {
            query.setLong(paramTwo, (Long) valueTwo);
        } else if (valueTwo instanceof BigDecimal) {
            query.setBigDecimal(paramTwo, (BigDecimal) valueTwo);
        } else if (valueTwo instanceof Float) {
            query.setFloat(paramTwo, (Float) valueTwo);
        } else if (valueTwo instanceof Double) {
            query.setDouble(paramTwo, (Double) valueTwo);
        } else if (valueTwo instanceof Timestamp) {
            query.setTimestamp(paramTwo, (Timestamp) valueTwo);
        } else if (valueTwo instanceof Date) {
            query.setDate(paramTwo, (Date) valueTwo);
        } else {
            query.setLong(paramTwo, getIdObject(valueTwo));
        }

    }

    private Long getIdObject(Object object) {
        Method getIdMethod = null;
        for (Method method : object.getClass().getMethods()) {
            if (method.getName().equals("getId")) {
                getIdMethod = method;
                break;
            }
        }
        Preconditions.isNotNull(getIdMethod, "No se encontro el atributo ID para el objecto [[" + object + "]]");

        Long id = null;
        try {
            id = (Long) getIdMethod.invoke(object);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
        }

        Preconditions.isNotNull(id, "El valor del atributo ID es null");
        return id;
    }

    public Field getAttribute(String atribute) {
        String[] parts = atribute.split("\\.");
        Preconditions.isFalse(parts.length > 2, "La columna [[" + atribute + "]] tiene muchos alias padres");

        Field field;
        if (parts.length == 2) {
            TableQuery parent = mapQueryTables.get(parts[0]);
            Preconditions.isNotNull(parent, "El filter no ubica una tabla con el alias [[" + parts[0] + "]]");
            field = getField(parent.getClazz(), parts[1], "No existe el atributo [[" + parts[1] + "]] para el tabla de alias [[" + parts[0] + "]]");
        } else {
            TableQuery tq = mapQueryTables.get(parts[0]);
            if (tq == null) {
                field = getField(mainTable.getClazz(), parts[0], "No existe el atributo [[" + parts[0] + "]] en la tabla principal");
            } else {
                TableQuery child = tq.getChild();
                String table = child.getName() == null ? child.getClazz().getSimpleName() : child.getName();
                field = getField(child.getClazz(), tq.getAttribute(), "No existe el atributo [[" + tq.getAttribute() + "]] con el alias [[" + parts[0] + "]] en la tabla [[" + table + "]]");
            }
        }
        return field;
    }

    private Field getField(Class clazz, String field, String error) {
        Field f;
        try {
            f = clazz.getDeclaredField(field);
        } catch (NoSuchFieldException ex) {
            throw new OctaviaException(error);
        }
        return f;
    }

    @Override
    public String toString() {
        Preconditions.isTrue(this.blockFilter == null, "No cerró todos los bloques");
        verifyExists();

        StringBuilder sql = new StringBuilder();
        if (isUpdate) {
            createUpdate(sql);
        } else {
            createSelect(sql);
        }
        createFrom(sql);
        if (isUpdate) {
            createSetOfUpdate(sql);
        }
        createWhere(prefix, sql, filters, BLOCK_AND);
        if (!isUpdate) {
            createGroupBy(sql);
            createOrderBy(sql);
        }

        if (prefix.equals("")) {
            String[] strings = sql.toString().split(enter);
            for (int i = 0; i < strings.length; i++) {
                if (i == 0) {
                    logger.debug("> " + strings[i]);
                } else {
                    logger.debug("  " + strings[i]);
                }
            }
        }

        return sql.toString();
    }

    private void verifyExists() {
        if (existsStatus == null) {
            return;
        }
        Preconditions.isFalse(existsStatus.equals("OPEN"), "Existe una clausula EXISTS sin conexion al query superior");
    }

    private void createUpdate(StringBuilder sql) {
        sql.append(" update ");
    }

    private void createSetOfUpdate(StringBuilder sql) {
        int cols = 0;
        sql.append(" set ");

        for (FilterQuery filter : sets) {
            String param = ":PARAM_" + String.format("%06d", filter.getIndex());

            if (cols > 0) {
                sql.append(", ").append(enter);
            }
            sql.append(filter.getAlias()).append(".").append(filter.getColumn());
            sql.append(" = ").append(param);

            ++cols;
        }
        sql.append(enter);
    }

    private void createSelect(StringBuilder sql) {
        if (isClauseExistsOpen) {
            sql.append(prefix).append(" select ");
            sql.append(mainTable.getAlias()).append(".id").append(enter);
        }
        if (selects == null && !isClauseExistsOpen) {
            List<TableQuery> parents = mainTable.getParents();
            parents.forEach(parent -> {
                setFetch(parent);
            });
            return;
        }
        if (selects == null && isClauseExistsOpen) {
            return;
        }

        TableQuery table = mapQueryTables.get(selects);
        if (table == null && mainTable.getAlias().equals(selects)) {
            table = mainTable;
        }
        if (table != null && !count) {
            List<TableQuery> parents = table.getParents();
            parents.forEach(parent -> {
                setFetch(parent);
            });
        }

        sql.append(prefix).append(" select ");
        String[] selectItems = selects.split(separator);
        for (String item : selectItems) {
            if (!isFormula(item)) {
                getAttribute(item);
            }
        }
        selects = selects.replaceAll(separator, ",");
        if (clazzInto != null) {
            sql.append("new ").append(clazzInto.getName()).append(" (").append(enterInto);
        }
        if (count) {
            sql.append("count(");
        }
        sql.append(distinct ? "distinct " : "").append(selects);
        if (count) {
            sql.append(") ");
        }
        sql.append(enter);

        if (clazzInto != null) {
            sql.append(prefix).append(" )").append(enter);
        }
    }

    private void setFetch(TableQuery table) {
        table.setFetch(true);
        List<TableQuery> parents = table.getParents();
        parents.forEach(parent -> {
            setFetch(parent);
        });
    }

    private void createWhere(String prefix, StringBuilder sql, List<FilterQuery> filters, FilterQuery.FilterTypeEnum typeCondition) {

        int loop = 0;
        String condition = typeCondition == BLOCK_AND ? " and " : " or ";

        for (FilterQuery filter : filters) {

            sql.append(prefix).append(!isClauseWhereOpen ? " where " : (loop == 0 ? "" : condition));
            isClauseWhereOpen = true;

            if (filter.getFilterType() == COMPLEX || filter.getFilterType() == COMPLEX_LIKE) {
                sql.append(filter.getColumn());
            } else if (filter.getFilterType() == EXISTS) {
                sql.append("exists (").append(enter);
            } else if (filter.getFilterType() == NOT_EXISTS) {
                sql.append("not exists (").append(enter);
            } else if (filter.getFilterType() == OTHER_CONDITION) {

            } else if (filter.getFilterType() == IN_LIST_DATES) {
                sql.append("date_format(");
                sql.append(filter.getAlias()).append(".").append(filter.getColumn());
                sql.append(",'%d/%m/%Y')");
            } else if (filter.getFilterType() == NOT_IN_LIST_DATES) {
                sql.append("date_format(");
                sql.append(filter.getAlias()).append(".").append(filter.getColumn());
                sql.append(",'%d/%m/%Y')");
            } else if (filter.getFilterType() == LIKE && filter.getColumnType() == Date.class) {
                sql.append("date_format(");
                sql.append(filter.getAlias()).append(".").append(filter.getColumn());
                sql.append(",'%d/%m/%Y')");
            } else if (filter.getFilterType() == NOT_LIKE && filter.getColumnType() == Date.class) {
                sql.append("date_format(");
                sql.append(filter.getAlias()).append(".").append(filter.getColumn());
                sql.append(",'%d/%m/%Y')");
            } else if (filter.getFilterType() == BLOCK_AND) {
            } else if (filter.getFilterType() == BLOCK_OR) {
            } else {
                sql.append(filter.getAlias()).append(".").append(filter.getColumn());
            }

            switch (filter.getFilterType()) {
                case GENERIC_OPERATOR:
                case COMPLEX:
                case COMPLEX_LIKE:
                case LIKE:
                case NOT_LIKE:
                case IN_LIST:
                case IN_LIST_DATES:
                case NOT_IN_LIST:
                case NOT_IN_LIST_DATES:
                    sql.append(" ").append(filter.getComparision().getValue());
                    sql.append(" :PARAM_").append(String.format("%06d", filter.getIndex()));
                    break;
                case IS_NULL:
                    sql.append(" IS NULL");
                    break;
                case IS_NOT_NULL:
                    sql.append(" IS NOT NULL");
                    break;
                case OTHER_CONDITION:
                    sql.append(filter.getColumn());
                    break;
                case NOT_EXISTS:
                case EXISTS:

                    sql.append(filter.getSubQuery().toString());
                    break;
                case NOT_IN_QUERY:
                case IN_QUERY:
                    sql.append(" ").append(filter.getComparision().getValue());
                    sql.append(" (").append(enter);
                    sql.append(filter.getSubQuery().toString());
                    break;
                case BETWEEN_IN:
                case BETWEEN_NOT_IN:
                    sql.append(" between");
                    sql.append(filter.getFilterType() == BETWEEN_NOT_IN ? " not" : "");
                    sql.append(" in");
                    sql.append(" :PARAM_").append(String.format("%06d", filter.getIndex()));
                    sql.append(" and :PARAM_").append(String.format("%06d", filter.getIndexTwo()));
                    break;
                case BLOCK_AND:
                case BLOCK_OR:
                    sql.append(" (").append(enter);
                    createWhere(prefix + "\t", sql, filter.getChildrenFilters(), filter.getFilterType());
                    break;
                default:
                    break;
            }

            if (Arrays.asList(EXISTS, IN_QUERY, NOT_EXISTS, NOT_IN_QUERY, BLOCK_AND, BLOCK_OR).contains(filter.getFilterType())) {
                sql.append(prefix).append(")");
            }

            sql.append(enter);

            loop++;
        }
    }

    private void createFrom(StringBuilder sql) {
        sql.append(prefix).append(" from ").append(mainTable.getClazz().getSimpleName()).append(" as ").append(mainTable.getAlias()).append(enter);

        List<TableQuery> tables = new ArrayList(mapQueryTables.values());
        for (TableQuery table : tables) {
            if (table.getType() == TypeJoinEnum.INNER_JOIN) {
                sql.append(prefix).append(" inner join ").append(table.isFetch() ? "fetch " : "");
                sql.append(table.getName()).append(" as ").append(table.getAlias()).append(enter);
            }
            if (table.getType() == TypeJoinEnum.LEFT_JOIN) {
                sql.append(prefix).append(" left join ").append(table.isFetch() ? "fetch " : "");
                sql.append(table.getName()).append(" as ").append(table.getAlias()).append(enter);
            }
        }
    }

    private void createOrderBy(StringBuilder sql) {
        if (itemsOrderBy.isEmpty()) {
            return;
        }

        int loop = 0;
        sql.append(prefix).append(" order by ");
        for (ItemOrderBy item : itemsOrderBy) {
            sql.append((loop == 0) ? "" : ", ");
            sql.append(item.toString());
            loop++;
        }
        sql.append(enter);
    }

    private void createGroupBy(StringBuilder sql) {
        if (groupBys == null) {
            return;
        }

        sql.append(prefix).append(" group by ").append(groupBys);
        sql.append(enter);
    }

    private ComparisonOperatorEnum findComparison(String comparison) {
        Preconditions.isNotNull(comparison, "El operador comparativo del filter no puede ser null");
        Preconditions.isNotBlank(comparison, "El operador comparativo del filter debe contener un valor");
        comparison = comparison.trim().toLowerCase().replaceAll(" +", " ");
        return ComparisonOperatorEnum.get(comparison);
    }

    private boolean isFormula(String formula) {
        formula = formula.trim();
        List<String> signs = Arrays.asList("(", "+", "-", "*", "/");
        for (String sign : signs) {
            if (formula.contains(sign)) {
                return true;
            }
        }
        return false;
    }

    private boolean isCorrectFormula(String formula) {

        for (int i = 0; i < formula.length(); i++) {
            char ch = formula.charAt(i);
            if (ch == '(') {
                String subFormula = formula.substring(i + 1);
                int pos = findEndFormula(subFormula);
                if (pos == -1) {
                    return false;
                }
                i += pos + 1;
                continue;
            }
            if (ch == ')') {
                return false;
            }
        }

        return true;
    }

    private boolean isNumeric(String str) {
        return str.matches("-?\\d+");
    }

    private int findEndFormula(String formula) {
        for (int i = 0; i < formula.length(); i++) {
            char ch = formula.charAt(i);
            if (ch == ')') {
                return i;
            }
            if (ch == '(') {
                String subFormula = formula.substring(i + 1);
                int pos = findEndFormula(subFormula);
                if (pos == -1) {
                    return pos;
                }
                i += pos + 1;
            }
        }
        return -1;
    }

    public void copyFrom(Octavia sqlx) {
        List<FilterQuery> filters = sqlx.filters;
        for (FilterQuery filter : filters) {
            this.filters.add(new FilterQuery(filter));
        }

        List<ItemOrderBy> itemsOrderBy = sqlx.itemsOrderBy;
        for (ItemOrderBy itemOrderBy : itemsOrderBy) {
            this.itemsOrderBy.add(itemOrderBy);
        }

        Map<String, TableQuery> mapQueryTables = sqlx.mapQueryTables;
        for (Map.Entry<String, TableQuery> entry : mapQueryTables.entrySet()) {
            this.mapQueryTables.put(entry.getKey(), entry.getValue());
        }

        this.selects = sqlx.selects;
        this.groupBys = sqlx.groupBys;
        this.existsStatus = sqlx.existsStatus;
        this.aliasCount = sqlx.aliasCount;
        this.firstResult = sqlx.firstResult;
        this.maxResults = sqlx.maxResults;
        this.parametersCount = sqlx.parametersCount;
        this.selectsCount = sqlx.selectsCount;
        this.mainTable = sqlx.mainTable;
        this.clazzInto = sqlx.clazzInto;
        this.query = sqlx.query;
        this.prefix = sqlx.prefix;
        this.activeSubquery = sqlx.activeSubquery;
        this.blockFilter = sqlx.blockFilter;
        this.childrenFilters = sqlx.childrenFilters;
        this.distinct = sqlx.distinct;
        this.count = sqlx.count;
        this.isClauseWhereOpen = sqlx.isClauseWhereOpen;
        this.isSubqueryIncluded = sqlx.isSubqueryIncluded;
        this.isClauseExistsOpen = sqlx.isClauseExistsOpen;
        this.isClauseInQueryOpen = sqlx.isClauseInQueryOpen;
    }

}
