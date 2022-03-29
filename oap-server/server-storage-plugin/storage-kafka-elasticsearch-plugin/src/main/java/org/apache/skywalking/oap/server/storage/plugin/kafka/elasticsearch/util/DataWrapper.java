package org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util;

public class DataWrapper {
    private Object data;
    private int scopeId;
    private String moduleName;
    private boolean update;
    private String id;
    private String dataSourceCode;
    public DataWrapper() {
    }
    public DataWrapper(Object data, String moduleName, boolean update, String id, int scopeId) {
        this.data = data;
        this.moduleName = moduleName;
        this.update = update;
        this.id = id;
        this.scopeId = scopeId;
    }

    public int getScopeId() {
        return scopeId;
    }

    public void setScopeId(int scopeId) {
        this.scopeId = scopeId;
    }

    public String getDataSourceCode() {
        return dataSourceCode;
    }

    public void setDataSourceCode(String dataSourceCode) {
        this.dataSourceCode = dataSourceCode;
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
