package org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.base;

public class DataWrapper {
    private Object data;
    private String moduleName;
    private boolean update;
    private String id;

    public DataWrapper() {
    }

    public DataWrapper(Object data, String moduleName) {
        this.data = data;
        this.moduleName = moduleName;
    }

    public DataWrapper(Object data, String moduleName, boolean update) {
        this.data = data;
        this.moduleName = moduleName;
        this.update = update;
    }

    public DataWrapper(Object data, String moduleName, boolean update, String id) {
        this.data = data;
        this.moduleName = moduleName;
        this.update = update;
        this.id = id;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public boolean isUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
