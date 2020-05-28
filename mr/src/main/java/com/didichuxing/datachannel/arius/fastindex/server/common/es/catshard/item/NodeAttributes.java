package com.didichuxing.datachannel.arius.fastindex.server.common.es.catshard.item;

import com.alibaba.fastjson.annotation.JSONField;

public class NodeAttributes {
    @JSONField(name = "rack")
    private String rack;
    @JSONField(name = "set")
    private String set;
    @JSONField(name = "max_local_storage_nodes")
    private long maxLocalStorageNodes;

    @JSONField(name = "master")
    private boolean master;
    @JSONField(name = "data")
    private boolean data;
    @JSONField(name = "client")
    private boolean client;

    public String getRack() {
        return rack;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }

    public long getMaxLocalStorageNodes() {
        return maxLocalStorageNodes;
    }

    public void setMaxLocalStorageNodes(long maxLocalStorageNodes) {
        this.maxLocalStorageNodes = maxLocalStorageNodes;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }


    public boolean isData() {
        return data;
    }

    public void setData(boolean data) {
        this.data = data;
    }

    public boolean isClient() {
        return client;
    }

    public void setClient(boolean client) {
        this.client = client;
    }
}
