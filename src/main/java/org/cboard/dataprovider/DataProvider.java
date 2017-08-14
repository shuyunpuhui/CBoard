package org.cboard.dataprovider;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.googlecode.aviator.AviatorEvaluator;
import org.cboard.dataprovider.aggregator.Aggregatable;
import org.cboard.dataprovider.aggregator.InnerAggregator;
import org.cboard.dataprovider.config.AggConfig;
import org.cboard.dataprovider.config.CompositeConfig;
import org.cboard.dataprovider.config.ConfigComponent;
import org.cboard.dataprovider.config.DimensionConfig;
import org.cboard.dataprovider.expression.NowFunction;
import org.cboard.dataprovider.result.AggregateResult;
import org.cboard.util.NaturalOrderComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by zyong on 2017/1/9.
 */
public abstract class DataProvider {

    private static final String RESULT_EMPTY_DEFAULT = null;

    private InnerAggregator innerAggregator;
    protected Map<String, String> dataSource;
    protected Map<String, String> query;
    private int resultLimit;
    private long interval = 12 * 60 * 60; // second

    public static final String NULL_STRING = "#NULL";
    private static final Logger logger = LoggerFactory.getLogger(DataProvider.class);

    static {
        AviatorEvaluator.addFunction(new NowFunction());
    }

    public abstract boolean doAggregationInDataSource();

    /**
     * get the aggregated data by user's widget designer
     *
     * @return
     */
    public final AggregateResult getAggData(AggConfig ac, boolean reload) throws Exception {
        evalValueExpression(ac);
        if (this instanceof Aggregatable && doAggregationInDataSource()) {
            return ((Aggregatable) this).queryAggData(ac);
        } else {
            checkAndLoad(reload);
            return innerAggregator.queryAggData(ac);
        }
    }

    public final String getViewAggDataQuery(AggConfig config) throws Exception {
        evalValueExpression(config);
        if (this instanceof Aggregatable && doAggregationInDataSource()) {
            return ((Aggregatable) this).viewAggDataQuery(config);
        } else {
            return "Not Support";
        }
    }

    /**
     * Get the options values of a dimension column
     *
     * @param columnName
     * @return
     */
    public final String[] getDimVals(String columnName, AggConfig config, boolean reload) throws Exception {
        String[] dimVals = null;
        evalValueExpression(config);
        if (this instanceof Aggregatable && doAggregationInDataSource()) {
            dimVals = ((Aggregatable) this).queryDimVals(columnName, config);
        } else {
            checkAndLoad(reload);
            dimVals = innerAggregator.queryDimVals(columnName, config);
        }
        return Arrays.stream(dimVals)
                .map(member -> {
                    return Objects.isNull(member) ? NULL_STRING : member;
                })
                .sorted(new NaturalOrderComparator()).limit(1000).toArray(String[]::new);
    }

    public final String[] getColumn(boolean reload) throws Exception {
        String[] columns = null;
        if (this instanceof Aggregatable && doAggregationInDataSource()) {
            columns = ((Aggregatable) this).getColumn();
        } else {
            checkAndLoad(reload);
            columns = innerAggregator.getColumn();
        }
        Arrays.sort(columns);
        return columns;
    }

    private void checkAndLoad(boolean reload) throws Exception {
        String key = getLockKey(dataSource, query);
        synchronized (key.intern()) {
            if (reload || !innerAggregator.checkExist()) {
                String[][] data = getData();
                innerAggregator.loadData(data, interval);
                logger.info("loadData {}", key);
            }
        }
    }

    private void evalValueExpression(AggConfig ac) {
        if (ac == null) {
            return;
        }
        ac.getFilters().forEach(e -> evaluator(e));
        ac.getColumns().forEach(e -> evaluator(e));
        ac.getRows().forEach(e -> evaluator(e));
    }

    private void evaluator(ConfigComponent e) {
        if (e instanceof DimensionConfig) {
            DimensionConfig dc = (DimensionConfig) e;
            dc.setValues(dc.getValues().stream().map(v -> getFilterValue(v)).collect(Collectors.toList()));
        }
        if (e instanceof CompositeConfig) {
            CompositeConfig cc = (CompositeConfig) e;
            cc.getConfigComponents().forEach(_e -> evaluator(_e));
        }
    }

    private String getFilterValue(String value) {
        if (value == null || !(value.startsWith("{") && value.endsWith("}"))) {
            return value;
        }
        return AviatorEvaluator.compile(value.substring(1, value.length() - 1), true).execute().toString();
    }

    private String getLockKey(Map<String, String> dataSource, Map<String, String> query) {
        return Hashing.md5().newHasher().putString(JSONObject.toJSON(dataSource).toString() + JSONObject.toJSON(query).toString(), Charsets.UTF_8).hash().toString();
    }

    public List<DimensionConfig> filterCCList2DCList(List<ConfigComponent> filters) {
        List<DimensionConfig> result = new LinkedList<>();
        filters.stream().forEach(cc -> {
            result.addAll(configComp2DimConfigList(cc));
        });
        return result;
    }

    public List<DimensionConfig> configComp2DimConfigList(ConfigComponent cc) {
        List<DimensionConfig> result = new LinkedList<>();
        if (cc instanceof DimensionConfig) {
            result.add((DimensionConfig) cc);
        } else {
            Iterator<ConfigComponent> iterator = cc.getIterator();
            while (iterator.hasNext()) {
                ConfigComponent next = iterator.next();
                result.addAll(configComp2DimConfigList(next));
            }
        }
        return result;
    }

    abstract public String[][] getData() throws Exception;

    public void setDataSource(Map<String, String> dataSource) {
        this.dataSource = dataSource;
    }

    public void setQuery(Map<String, String> query) {
        this.query = query;
    }

    public void setResultLimit(int resultLimit) {
        this.resultLimit = resultLimit;
    }

    public int getResultLimit() {
        return resultLimit;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void setInnerAggregator(InnerAggregator innerAggregator) {
        this.innerAggregator = innerAggregator;
    }

    // added by shph
    public String[][] getOrderedAggResult(TreeMap<String, Object> aggTreeMap, int dimCount, String[] checkRes)
            throws Exception {
        String minYear = checkRes[0];
        String maxYear = checkRes[1];
        String minMonth = checkRes[2];
        String maxMonth = checkRes[3];
        String onlyYear = checkRes[4];

        List<String[]> resultArrayList = new ArrayList<>();
        if (aggTreeMap == null || aggTreeMap.size() == 0) {
            return resultArrayList.toArray(new String[][]{});
        }

        if ("true".equals(onlyYear)) {
            return onlyYearResult(aggTreeMap, dimCount, checkRes);
        }

        if (minYear == null && minMonth != null) {
            throw new Exception("无法计算，已经配置了过滤月份，请配置过滤的年份");
        } else if (minYear != null && minMonth == null) {
            minMonth = "01";
            maxMonth = "12";
        }

        String filterResultMinYm = minYear != null ? minYear + minMonth : null;
        String filterResultMaxYm = minYear != null ? maxYear + maxMonth : null;

        String firstKey = aggTreeMap.firstEntry().getKey();
        Date firstYearMonth = new SimpleDateFormat("yyyyMM").parse(firstKey);

        String lastKey = aggTreeMap.lastEntry().getKey();
        Date lastYearMonth = new SimpleDateFormat("yyyyMM").parse(lastKey);
        Calendar lastNextCalendar = Calendar.getInstance();
        lastNextCalendar.setTime(lastYearMonth);
        lastNextCalendar.add(Calendar.MONTH, 1);

        Calendar iteratorCalendar = Calendar.getInstance();
        iteratorCalendar.setTime(firstYearMonth);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        DecimalFormat decimalFormat=new DecimalFormat(".00");

        while (iteratorCalendar.getTime().before(lastNextCalendar.getTime())) {
            // 获取当前月字符串
            String currYearMonth = sdf.format(iteratorCalendar.getTime());

            // 获取上个月
            Calendar previousMonthCalendar = Calendar.getInstance();
            previousMonthCalendar.setTime(iteratorCalendar.getTime());
            previousMonthCalendar.add(Calendar.MONTH, -1);
            String previousYearMonth = sdf.format(previousMonthCalendar.getTime());

            // 获取去年的
            Calendar lastYearCalendar = Calendar.getInstance();
            lastYearCalendar.setTime(iteratorCalendar.getTime());
            lastYearCalendar.add(Calendar.YEAR, -1);
            String lastYear = sdf.format(lastYearCalendar.getTime());

            // 计算
            Object currRowObj = aggTreeMap.get(currYearMonth);
            // 如果不做过滤，或者做了过滤，只保留比最小值大的记录
            if (filterResultMinYm == null || (Integer.parseInt(filterResultMinYm) <= Integer.parseInt(currYearMonth)
                    && Integer.parseInt(currYearMonth) <= Integer.parseInt(filterResultMaxYm))) {
                if (currRowObj == null) {
                    String[] newRow = getEmptyRow(currYearMonth, dimCount);
                    resultArrayList.add(newRow);
                } else {
                    String[] currRowObj1 = (String[]) currRowObj;
                    String[] currRow = Arrays.copyOf(currRowObj1, currRowObj1.length);

                    float curr = Float.parseFloat(currRow[2]);

                    // 环比
                    Object momRowObj = aggTreeMap.get(previousYearMonth);
                    if (momRowObj == null) {
                        currRow[2] = RESULT_EMPTY_DEFAULT;
                    } else {
                        String[] momRow = (String[]) momRowObj;
                        float previous = Float.parseFloat(momRow[2]);

                        if ((int) previous != 0) {
                            float mom = ((curr - previous) * 100) / previous;
                            currRow[2] = decimalFormat.format(mom);
                        } else {
                            currRow[2] = RESULT_EMPTY_DEFAULT;
                        }
                    }

                    // 同比
                    Object yoyRowObj = aggTreeMap.get(lastYear);
                    if (currRow.length >= 4) {
                        if (yoyRowObj == null) {
                            currRow[3] = RESULT_EMPTY_DEFAULT;
                        } else {
                            String[] yoyRow = (String[]) yoyRowObj;
                            float previousYear = Float.parseFloat(yoyRow[2]);

                            if ((int) previousYear != 0) {
                                float yoy = ((curr - previousYear) * 100) / previousYear;
                                currRow[3] = decimalFormat.format(yoy);
                            } else {
                                currRow[3] = RESULT_EMPTY_DEFAULT;
                            }
                        }
                    }

                    resultArrayList.add(currRow);
                }
            }

            // 最后递增月份，直到结束
            iteratorCalendar.add(Calendar.MONTH, 1);
        }

        return resultArrayList.toArray(new String[][]{});
    }

    private String[][] onlyYearResult(TreeMap<String, Object> aggTreeMap, int dimCount, String[] checkRes)
            throws Exception {
        // 算按年的
        String minYear = checkRes[0];
        String maxYear = checkRes[1];

        List<String[]> resultArrayList = new ArrayList<>();

        String firstKey = aggTreeMap.firstEntry().getKey();
        Date firstYear = new SimpleDateFormat("yyyy").parse(firstKey);

        String lastKey = aggTreeMap.lastEntry().getKey();
        Date lastYear = new SimpleDateFormat("yyyy").parse(lastKey);

        Calendar lastNextCalendar = Calendar.getInstance();
        lastNextCalendar.setTime(lastYear);
        lastNextCalendar.add(Calendar.YEAR, 1);

        Calendar iteratorCalendar = Calendar.getInstance();
        iteratorCalendar.setTime(firstYear);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
        DecimalFormat decimalFormat=new DecimalFormat(".00");

        while (iteratorCalendar.getTime().before(lastNextCalendar.getTime())) {
            String currYear = sdf.format(iteratorCalendar.getTime());
            String previousYear = ((Integer)(Integer.parseInt(currYear) - 1)).toString();

            Object currRowObj = aggTreeMap.get(currYear);
            if (minYear == null || (Integer.parseInt(minYear) <= Integer.parseInt(currYear)
                    && Integer.parseInt(currYear) <= Integer.parseInt(maxYear))) {

                if (currRowObj == null) {
                    String[] newRow = getEmptyRowOnlyYear(currYear, dimCount);
                    resultArrayList.add(newRow);
                } else {
                    String[] currRowObj1 = (String[]) currRowObj;
                    String[] currRow = Arrays.copyOf(currRowObj1, currRowObj1.length);

                    float curr = Float.parseFloat(currRow[1]);

                    Object rowObj = aggTreeMap.get(previousYear);
                    if (rowObj == null) {
                        currRow[1] = RESULT_EMPTY_DEFAULT;
                    } else {
                        String[] row = (String[]) rowObj;
                        float previous = Float.parseFloat(row[1]);

                        if ((int) previous != 0) {
                            float mom = ((curr - previous) * 100) / previous;
                            currRow[1] = decimalFormat.format(mom);
                        } else {
                            currRow[1] = RESULT_EMPTY_DEFAULT;
                        }
                    }

                    resultArrayList.add(currRow);
                }

            }

            // 最后递增月份，直到结束
            iteratorCalendar.add(Calendar.YEAR, 1);
        }

        return resultArrayList.toArray(new String[][]{});
    }

    public String[] checkAndFilter(AggConfig config) throws Exception {

        List<DimensionConfig> rows = config.getRows();
        if (rows.size() == 0) {
            throw new Exception("必须设置行维的年或年月");
        }

        String onlyYear = "false";
        if (rows.size() == 1) {
            onlyYear = "true";
        }

        DimensionConfig yearDimConfig = rows.get(0);
        DimensionConfig monthDimConfig = null;
        if (rows.size() == 2) {
            monthDimConfig = rows.get(1);
        }

        List<ConfigComponent> filters = config.getFilters();
        String minYear = null;
        String minMonth = null;
        String maxYear = null;
        String maxMonth = null;

        for (ConfigComponent filter : filters) {
            if (filter instanceof DimensionConfig) {
                DimensionConfig dc = (DimensionConfig) filter;

                if (dc.getColumnName().equals(yearDimConfig.getColumnName())) {
                    List<String> values = dc.getValues();
                    minYear = Collections.min(values);
                    maxYear = Collections.max(values);
                    Integer minValueMinusOne = Integer.parseInt(minYear) - 1;

                    values.add(minValueMinusOne.toString());
                }

                if (monthDimConfig != null && dc.getColumnName().equals(monthDimConfig.getColumnName())) {
                    List<String> values = dc.getValues();
                    minMonth = Collections.min(values);
                    maxMonth = Collections.max(values);
                }
            }
        }

        return new String[]{minYear, maxYear, minMonth, maxMonth, onlyYear};
    }

    public String[] getEmptyRow(String yearMonth, int dimCount) {
        String year = yearMonth.substring(0, 4);
        String month = yearMonth.substring(4);

        if (dimCount == 3) {
            return new String[]{year, month, RESULT_EMPTY_DEFAULT};
        }

        if (dimCount == 4) {
            return new String[]{year, month, RESULT_EMPTY_DEFAULT, RESULT_EMPTY_DEFAULT};
        }

        if (dimCount == 5) {
            return new String[]{year, month, RESULT_EMPTY_DEFAULT, RESULT_EMPTY_DEFAULT, RESULT_EMPTY_DEFAULT};
        }

        return new String[]{year, month, RESULT_EMPTY_DEFAULT};
    }

    public String[] getEmptyRowOnlyYear(String year, int dimCount) {

        if (dimCount == 2) {
            return new String[]{year, RESULT_EMPTY_DEFAULT};
        }

        if (dimCount == 3) {
            return new String[]{year, RESULT_EMPTY_DEFAULT, RESULT_EMPTY_DEFAULT};
        }

        if (dimCount == 4) {
            return new String[]{year, RESULT_EMPTY_DEFAULT, RESULT_EMPTY_DEFAULT, RESULT_EMPTY_DEFAULT};
        }

        return new String[]{year, RESULT_EMPTY_DEFAULT};
    }
}
