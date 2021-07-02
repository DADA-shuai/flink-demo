package com.puhuilink.pojo;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/2 9:54
 * @description：
 * @modified By：
 * @version: $
 */
public class Metric {
    private Long host_id;
    private Long endpoint_id;
    private Long metric_id;
    private Double metric_value;
    private Long collect_time;
    private String agent;
    private String aggregate;
    private Long upload_time;
    private String metric_value_str;

    public Metric() {}

    public Metric(Long host_id, Long endpoint_id, Long metric_id, Double metric_value, Long collect_time, String agent, String aggregate, Long upload_time, String metric_value_str) {
        this.host_id = host_id;
        this.endpoint_id = endpoint_id;
        this.metric_id = metric_id;
        this.metric_value = metric_value;
        this.collect_time = collect_time;
        this.agent = agent;
        this.aggregate = aggregate;
        this.upload_time = upload_time;
        this.metric_value_str = metric_value_str;
    }

    @Override
    public String toString() {
        return "Metric{" +
            "host_id=" + host_id +
            ", endpoint_id=" + endpoint_id +
            ", metric_id=" + metric_id +
            ", metric_value=" + metric_value +
            ", collect_time=" + collect_time +
            ", agent='" + agent + '\'' +
            ", aggregate='" + aggregate + '\'' +
            ", upload_time=" + upload_time +
            ", metric_value_str='" + metric_value_str + '\'' +
            '}';
    }

    public Long getHost_id() {
        return host_id;
    }

    public void setHost_id(Long host_id) {
        this.host_id = host_id;
    }

    public Long getEndpoint_id() {
        return endpoint_id;
    }

    public void setEndpoint_id(Long endpoint_id) {
        this.endpoint_id = endpoint_id;
    }

    public Long getMetric_id() {
        return metric_id;
    }

    public void setMetric_id(Long metric_id) {
        this.metric_id = metric_id;
    }

    public Double getMetric_value() {
        return metric_value;
    }

    public void setMetric_value(Double metric_value) {
        this.metric_value = metric_value;
    }

    public Long getCollect_time() {
        return collect_time;
    }

    public void setCollect_time(Long collect_time) {
        this.collect_time = collect_time;
    }

    public String getAgent() {
        return agent;
    }

    public void setAgent(String agent) {
        this.agent = agent;
    }

    public String getAggregate() {
        return aggregate;
    }

    public void setAggregate(String aggregate) {
        this.aggregate = aggregate;
    }

    public Long getUpload_time() {
        return upload_time;
    }

    public void setUpload_time(Long upload_time) {
        this.upload_time = upload_time;
    }

    public String getMetric_value_str() {
        return metric_value_str;
    }

    public void setMetric_value_str(String metric_value_str) {
        this.metric_value_str = metric_value_str;
    }
}
